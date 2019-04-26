using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Newtonsoft.Json;

namespace GiantBombDataTool
{
    public sealed class LocalJsonTableStore : ITableStore, ITableStagingStore
    {
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

        public IEnumerable<string> GetTables()
        {
            foreach (var metadataFileName in Directory.EnumerateFiles(_storePath, "*.metadata.json"))
                yield return Path.GetFileName(metadataFileName).Split('.')[0];
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

        public string? WriteStagedEntities(string table, IEnumerable<TableEntity> entities)
        {
            var enumerator = entities.GetEnumerator();
            if (!enumerator.MoveNext())
                return null;

            var timestamp = enumerator.Current.Timestamp;
            string path = GetTableStagingPath(table, timestamp);

            using (var writer = new StreamWriter(path, append: false, Encoding.UTF8))
            {
                do
                {
                    var entity = enumerator.Current;
                    writer.WriteLine(JsonConvert.SerializeObject(entity.Content, _contentSettings));
                } while (enumerator.MoveNext());
            }

            return Path.GetFileName(path);
        }

        private string GetMetadataPath(string table) => Path.Combine(_storePath, $"{table}.metadata.json");
        private string GetStagingMetadataPath(string table) => Path.Combine(_storePath, $"{table}.staging.json");
        private string GetTableStagingPath(string table, DateTime timestamp)
            => Path.Combine(_storePath, $"{table}.{timestamp:yyyyMMddHHmmss}.jsonl");
    }
}
