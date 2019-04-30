using System;
using System.Collections;
using System.Collections.Generic;
using System.IO.Compression;
using System.Linq;
using GiantBombDataTool.Stores;

namespace GiantBombDataTool
{
    public sealed class FlowContext
    {
        public FlowContext(
            LocalJsonTableStore localStore,
            IReadOnlyTableStore remoteStore,
            IReadOnlyList<string> tables,
            Config config)
        {
            LocalStore = localStore;
            RemoteStore = remoteStore;
            Tables = tables;
            Config = config;
        }

        public LocalJsonTableStore LocalStore { get; }
        public IReadOnlyTableStore RemoteStore { get; }
        public IReadOnlyList<string> Tables { get; }
        public Config Config { get; }

        public FlowContext WithTables(IReadOnlyList<string> tables)
        {
            return new FlowContext(LocalStore, RemoteStore, tables, Config);
        }
    }

    public sealed class CloneFlow
    {
        private readonly FlowContext _context;

        public CloneFlow(FlowContext context)
        {
            _context = context;
        }

        public bool TryExecute()
        {
            IEnumerable<string> tables;
            if (_context.Tables.Count > 0)
                tables = _context.Tables;
            else
                tables = _context.Config.Tables.Keys;

            foreach (string table in tables)
            {
                if (!_context.Config.Tables.TryGetValue(table, out var tableConfig))
                {
                    Console.WriteLine($"Unknown table: {table}");
                    return false;
                }

                var metadata = new Metadata
                {
                    Config = tableConfig,
                };

                if (!_context.LocalStore.TryInitialize(table, metadata))
                    return false;

                var tableContext = _context.WithTables(new[] { table });

                var pullFlow = new PullFlow(tableContext);
                if (!pullFlow.TryExecute())
                    return false;
            }

            return true;
        }
    }

    public sealed class FetchFlow
    {
        private readonly FlowContext _context;

        public FetchFlow(FlowContext context)
        {
            _context = context;
        }

        public bool TryExecute()
        {
            IEnumerable<string> tables;
            if (_context.Tables.Count > 0)
                tables = _context.Tables;
            else
                tables = _context.LocalStore.GetTables();

            foreach (string table in tables)
            {
                if (!_context.LocalStore.TryLoadMetadata(table, out var tableMetadata))
                    return false;

                // NOTE: overrides are applied transiently (not persisted to store)
                tableMetadata.Config.OverrideWith(_context.Config);

                if (!_context.LocalStore.TryLoadStagingMetadata(table, out var stagingMetadata))
                {
                    stagingMetadata = new StagingMetadata
                    {
                        LastTimestamp = tableMetadata.LastTimestamp,
                        LastId = tableMetadata.LastId,
                    };
                }

                if (!TryFetchEntities(table, tableMetadata.Config, stagingMetadata))
                    return false;

                if (stagingMetadata.FetchDetail.Count > 0 && !TryFetchDetail(table, tableMetadata, stagingMetadata))
                    return false;
            }

            return true;
        }

        private bool TryFetchEntities(string table, TableConfig config, StagingMetadata metadata)
        {
            int chunkSize = config.ChunkSize ?? CommonConfig.DefaultChunkSize;

            // TODO: Currently writing in GetEntitiesByTimestamp, need to rethink status writes
            //string lastUpdateText = metadata.LastTimestamp != null ?
            //    metadata.LastTimestamp.Value.ToString("yyyy-MM-dd HH:mm:ss") :
            //    "forever";
            //Console.WriteLine($"Retrieving {table} updated since {lastUpdateText}");

            var entities = _context.RemoteStore.GetEntitiesByTimestamp(
                table,
                config,
                metadata.LastTimestamp,
                metadata.LastId);

            using var enumerator = new EntityEnumerator(entities);
            bool first = true;
            while (!enumerator.Finished)
            {
                var chunk = _context.LocalStore.WriteStagedEntities(table, enumerator.Take(chunkSize));
                if (chunk != null)
                {
                    metadata.LastTimestamp = enumerator.LastTimestamp;
                    metadata.LastId = enumerator.LastId;

                    if (config.DetailFields.Count > 0 && config.Detail != DetailBehavior.Skip)
                        metadata.FetchDetail.Add(chunk);
                    else
                        metadata.Merge.Add(chunk);

                    _context.LocalStore.SaveStagingMetadata(table, metadata);

                    Console.WriteLine($"Wrote {chunk} to staging");
                }
                else if (first)
                {
                    Console.WriteLine($"{table} already up to date.");
                }
                first = false;
            }
            return true;
        }

        private bool TryFetchDetail(string table, Metadata tableMetadata, StagingMetadata metadata)
        {
            var config = tableMetadata.Config;
            int chunkSize = config.ChunkSize ?? CommonConfig.DefaultDetailChunkSize;

            while (metadata.FetchDetail.Count > 0)
            {
                var chunk = metadata.FetchDetail.First();

                var entities = GetEntitiesToFetchDetail(table, chunk, config);

                // TODO: chunk this into smaller files or create smaller fetchDetail chunks in the first place (change the chunkSize in TryFetchEntities)
                var detailChunk = _context.LocalStore.WriteStagedEntities(table, entities, detailForChunk: chunk);
                if (detailChunk != null)
                {
                    metadata.Merge.Add(detailChunk);
                    Console.WriteLine($"Wrote {detailChunk} to staging");
                }

                metadata.FetchDetail.Remove(chunk);
                _context.LocalStore.SaveStagingMetadata(table, metadata);
                _context.LocalStore.RemoveStagedEntities(chunk);
            }

            if (config.Detail == DetailBehavior.Backfill)
            {
                var entities = _context.LocalStore.ReadEntities(table, tableMetadata);
                
                // TODO: finish implementation
                // - move entities into an iterator method that checks if any detailFields are missing
                // - refactor GetEntitiesToFetchDetail and reuse
                // - use chunking with EntityEnumerator to write staged entities
            }

            return true;
        }

        private IEnumerable<TableEntity> GetEntitiesToFetchDetail(string table, string chunk, TableConfig config)
        {
            foreach (var entity in _context.LocalStore.ReadStagedEntities(chunk))
            {
                yield return _context.RemoteStore.GetEntityDetail(
                    table,
                    config,
                    entity);
            }
        }

        private sealed class EntityEnumerator : IEnumerable<TableEntity>, IDisposable
        {
            private readonly IEnumerator<TableEntity> _entities;

            internal EntityEnumerator(IEnumerable<TableEntity> entities)
            {
                _entities = entities.GetEnumerator();
            }

            internal long LastId { get; private set; }
            internal DateTime LastTimestamp { get; private set; }
            internal bool Finished { get; private set; }

            public void Dispose()
            {
                _entities.Dispose();
            }

            public IEnumerator<TableEntity> GetEnumerator()
            {
                while (_entities.MoveNext())
                {
                    var entity = _entities.Current;
                    LastId = entity.Id;
                    LastTimestamp = entity.Timestamp;
                    yield return entity;
                }
                Finished = true;
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }
        }
    }

    public sealed class MergeFlow
    {
        private readonly FlowContext _context;

        public MergeFlow(FlowContext context)
        {
            _context = context;
        }

        public bool TryExecute()
        {
            // TODO: share code with FetchFlow??
            IEnumerable<string> tables;
            if (_context.Tables.Count > 0)
                tables = _context.Tables;
            else
                tables = _context.LocalStore.GetTables();

            foreach (string table in tables)
            {
                if (!_context.LocalStore.TryLoadMetadata(table, out var tableMetadata))
                    return false;

                if (!_context.LocalStore.TryLoadStagingMetadata(table, out var stagingMetadata))
                    continue;

                if (!TryMergeEntities(table, tableMetadata, stagingMetadata))
                    return false;
            }

            return true;
        }

        private bool TryMergeEntities(string table, Metadata tableMetadata, StagingMetadata stagingMetadata)
        {
            while (stagingMetadata.Merge.Count > 0)
            {
                var chunk = stagingMetadata.Merge.First();

                Console.Write($"Merging {table} from {chunk}... ");

                var entities = _context.LocalStore.ReadStagedEntities(chunk);
                if (!_context.LocalStore.TryUpsertEntities(table, tableMetadata, entities))
                    return false;

                stagingMetadata.Merge.Remove(chunk);
                if (stagingMetadata.Merge.Count > 0)
                {
                    _context.LocalStore.SaveStagingMetadata(table, stagingMetadata);
                }
                else
                {
                    tableMetadata.LastTimestamp = stagingMetadata.LastTimestamp;
                    tableMetadata.LastId = stagingMetadata.LastId;
                    _context.LocalStore.SaveMetadata(table, tableMetadata);
                    _context.LocalStore.RemoveStagingMetadata(table);
                }

                _context.LocalStore.RemoveStagedEntities(chunk);
            }
            return true;
        }
    }

    public sealed class PullFlow
    {
        private readonly FlowContext _context;

        public PullFlow(FlowContext context)
        {
            _context = context;
        }

        public bool TryExecute()
        {
            var fetchFlow = new FetchFlow(_context);
            if (!fetchFlow.TryExecute())
                return false;

            var mergeFlow = new MergeFlow(_context);
            if (!mergeFlow.TryExecute())
                return false;

            return true;
        }
    }

    public sealed class CompressFlow
    {
        private readonly FlowContext _context;
        private readonly CompressionMode _mode;

        public CompressFlow(FlowContext context, CompressionMode mode)
        {
            _context = context;
            _mode = mode;
        }

        public bool TryExecute()
        {
            // TODO: share code with FetchFlow??
            IEnumerable<string> tables;
            if (_context.Tables.Count > 0)
                tables = _context.Tables;
            else
                tables = _context.LocalStore.GetTables();

            foreach (string table in tables)
            {
                if (!_context.LocalStore.TryLoadMetadata(table, out var tableMetadata))
                    return false;

                var currentMode = tableMetadata.Config.Compression == TableCompressionKind.GZip ?
                    CompressionMode.Compress :
                    CompressionMode.Decompress;

                if (currentMode == _mode)
                {
                    Console.WriteLine($"{table} is already {currentMode.ToString().ToLowerInvariant()}ed");
                    continue;
                }

                Console.Write($"{_mode}ing {table}... ");

                var newMetadata = tableMetadata.Clone();

                newMetadata.Config.Compression = _mode == CompressionMode.Compress ?
                    TableCompressionKind.GZip :
                    TableCompressionKind.None;

                var entities = _context.LocalStore.ReadEntities(table, tableMetadata);
                _context.LocalStore.TryUpsertEntities(table, newMetadata, entities);

                _context.LocalStore.SaveMetadata(table, newMetadata);

                // TODO: this leaves behind the original table file, ideally would clean up
                // Would need an ITableStore method to truncate the table or something
                // However, maybe compression should just be handled by the store directly
                // -- I'm assuming here that reading and writing entities simultaneously
                // with different compression options is safe, but that only is true when
                // the store is using separate files for each.
            }

            return true;
        }
    }
}
