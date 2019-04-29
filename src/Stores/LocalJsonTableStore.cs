using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace GiantBombDataTool.Stores
{
    public enum TableCompressionKind
    {
        [EnumMember(Value = "none")]
        None,

        [EnumMember(Value ="gzip")]
        GZip,
    }

    public sealed class LocalJsonTableStore : ITableStore, ITableMetadataStore, ITableStagingStore
    {
        private static readonly Encoding _encoding = Encoding.UTF8;

        private static readonly JsonSerializerSettings _metadataSettings = new JsonSerializerSettings
        {
            Formatting = Formatting.Indented,
        };

        private static readonly JsonSerializerSettings _contentSettings = new JsonSerializerSettings
        {
            Formatting = Formatting.None,
        };

        private readonly string _storePath;

        public LocalJsonTableStore(string storePath)
        {
            _storePath = storePath;
        }

        public object Location => _storePath;

        public bool TryUpsertEntities(string table, Metadata metadata, IEnumerable<TableEntity> entities)
        {
            var compression = metadata.Config.Compression;

            string path = GetTablePath(table, compression);
            bool existing = File.Exists(path);

            string tempPath = GetTempTablePath(table, compression);

            using var entitiesEnum = entities.OrderBy(e => e.Id).GetEnumerator();
            if (!entitiesEnum.MoveNext())
                return true;

            var existingEntities = existing ?
                ReadExistingEntities(path, compression) :
                Enumerable.Empty<TableEntity>();

            long insertedEntities = 0;
            long updatedEntities = 0;

            using (var writer = CreateStreamWriter(tempPath, compression))
            using (var existingEntitiesEnum = existingEntities.GetEnumerator())
            {
                var existingEntity = existingEntitiesEnum.MoveNext() ?  existingEntitiesEnum.Current : null;

                TableEntity nextEntity;

                do
                {
                    nextEntity = entitiesEnum.Current;

                    while (existingEntity != null && existingEntity.Id < nextEntity.Id)
                    {
                        writer.WriteLine(JsonConvert.SerializeObject(existingEntity.Content, _contentSettings));
                        existingEntity = existingEntitiesEnum.MoveNext() ? existingEntitiesEnum.Current : null;
                    }

                    if (existingEntity != null && existingEntity.Id == nextEntity.Id)
                    {
                        if (existingEntity.Timestamp > nextEntity.Timestamp)
                        {
                            Console.WriteLine($"Found existing entity '{existingEntity.Id}' in {table} with later timestamp '{existingEntity.Timestamp}' than incoming timestamp '{nextEntity.Timestamp}'");
                            return false;
                        }

                        existingEntity = existingEntitiesEnum.MoveNext() ? existingEntitiesEnum.Current : null;
                        updatedEntities++;
                    }
                    else
                    {
                        insertedEntities++;
                    }

                    writer.WriteLine(JsonConvert.SerializeObject(nextEntity.Content, _contentSettings));
                } while (entitiesEnum.MoveNext());

                while (existingEntity != null)
                {
                    writer.WriteLine(JsonConvert.SerializeObject(existingEntity.Content, _contentSettings));
                    existingEntity = existingEntitiesEnum.MoveNext() ? existingEntitiesEnum.Current : null;
                }
            }

            if (existing)
                File.Delete(path);

            File.Move(tempPath, path);

            Console.WriteLine($"merged {insertedEntities} new and {updatedEntities} existing entities to {table}");

            return true;
        }

        public IEnumerable<TableEntity> ReadEntities(string table, Metadata metadata)
        {
            var compression = metadata.Config.Compression;

            string path = GetTablePath(table, compression);

            return File.Exists(path) ?
                ReadExistingEntities(path, compression) :
                Enumerable.Empty<TableEntity>();
        }

        public IEnumerable<string> GetTables()
        {
            foreach (var metadataFileName in Directory.EnumerateFiles(_storePath, "*.metadata.json"))
                yield return Path.GetFileName(metadataFileName).Split('.')[0];
        }

        public bool TryInitialize(string table, Metadata metadata)
        {
            Directory.CreateDirectory(_storePath);

            string metadataPath = GetMetadataPath(table);

            if (File.Exists(metadataPath) || Directory.Exists(metadataPath))
            {
                Console.WriteLine($"Table already initialized: {metadataPath}");
                return false;
            }

            File.WriteAllText(
                metadataPath,
                JsonConvert.SerializeObject(metadata, _metadataSettings));

            Console.WriteLine($"Initialized table: {metadataPath}");

            return true;
        }

        public bool TryLoadMetadata(string table, out Metadata metadata)
        {
            string path = GetMetadataPath(table);
            if (!File.Exists(path))
            {
                Console.WriteLine($"Table metadata not found: {path}");
                metadata = null!;
                return false;
            }

            metadata = JsonConvert.DeserializeObject<Metadata>(
                File.ReadAllText(path),
                _metadataSettings);
            return true;
        }

        public void SaveMetadata(string table, Metadata metadata)
        {
            // TODO: make all file write operations atomic
            string path = GetMetadataPath(table);
            File.WriteAllText(path, JsonConvert.SerializeObject(metadata, _metadataSettings));
        }

        public bool TryLoadStagingMetadata(string table, out StagingMetadata metadata)
        {
            string path = GetStagingMetadataPath(table);
            if (!File.Exists(path))
            {
                metadata = null!;
                return false;
            }

            metadata = JsonConvert.DeserializeObject<StagingMetadata>(
                File.ReadAllText(path),
                _metadataSettings);
            return true;
        }

        public void SaveStagingMetadata(string table, StagingMetadata metadata)
        {
            string path = GetStagingMetadataPath(table);
            File.WriteAllText(path, JsonConvert.SerializeObject(metadata, _metadataSettings));
        }

        public void RemoveStagingMetadata(string table)
        {
            string path = GetStagingMetadataPath(table);
            File.Delete(path);
        }

        public string? WriteStagedEntities(string table, IEnumerable<TableEntity> entities)
        {
            var enumerator = entities.GetEnumerator();
            if (!enumerator.MoveNext())
                return null;

            var timestamp = enumerator.Current.Timestamp;
            string path = GetTableStagingPath(table, timestamp);

            using (var writer = CreateStreamWriter(path))
            {
                do
                {
                    var entity = enumerator.Current;
                    writer.WriteLine(JsonConvert.SerializeObject(entity.Content, _contentSettings));
                } while (enumerator.MoveNext());
            }

            return Path.GetFileName(path);
        }

        public IEnumerable<TableEntity> ReadStagedEntities(string chunk)
        {
            string path = GetTableStagingPath(chunk);
            return ReadExistingEntities(path);
        }

        public void RemoveStagedEntities(string chunk)
        {
            string path = GetTableStagingPath(chunk);
            File.Delete(path);
        }

        private IEnumerable<TableEntity> ReadExistingEntities(string path, TableCompressionKind? compression = null)
        {
            using (var reader = CreateStreamReader(path, compression))
            {
                string content;
                while ((content = reader.ReadLine()) != null)
                {
                    yield return new TableEntity(JsonConvert.DeserializeObject<JObject>(content, _contentSettings));
                }
            }
        }

        private StreamReader CreateStreamReader(string path, TableCompressionKind? compression = null)
        {
            Stream stream = File.Open(path, FileMode.Open, FileAccess.Read, FileShare.None);

            if (compression == TableCompressionKind.GZip)
                stream = new GZipStream(stream, CompressionMode.Decompress);

            return new StreamReader(stream, _encoding);
        }

        private StreamWriter CreateStreamWriter(string path, TableCompressionKind? compression = null)
        {
            Stream stream = File.Open(path, FileMode.Create, FileAccess.Write, FileShare.None);

            if (compression == TableCompressionKind.GZip)
                stream = new GZipStream(stream, CompressionMode.Compress);

            return new StreamWriter(stream, _encoding);
        }

        private string GetTablePath(string table, TableCompressionKind? compression)
        {
            return Path.Combine(_storePath, $"{table}.jsonl") + GetCompressionSuffix(compression);
        }

        private string GetTempTablePath(string table, TableCompressionKind? compression)
        {
            return Path.Combine(_storePath, $"{table}.temp.jsonl") + GetCompressionSuffix(compression);
        }

        private string GetCompressionSuffix(TableCompressionKind? compression)
        {
            return compression == TableCompressionKind.GZip ? ".gz" : string.Empty;
        }

        private string GetMetadataPath(string table) => Path.Combine(_storePath, $"{table}.metadata.json");
        private string GetStagingMetadataPath(string table) => Path.Combine(_storePath, $"{table}.staging.json");
        private string GetTableStagingPath(string table, DateTime timestamp)
            => Path.Combine(_storePath, $"{table}.{timestamp:yyyyMMddHHmmss}.jsonl");
        private string GetTableStagingPath(string chunk)
            => Path.Combine(_storePath, chunk);
    }
}
