using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO.Compression;
using System.Linq;
using Talys.Stores;

namespace Talys
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

            // TODO: create all table metadata upfront, so that subsequent "pull" will complete interrupted clone
            // separate the init code here into an "init" command ("clone" is just init + pull)
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

                // TODO: inherit fields/detailFields from base config if not specified in metadata config

                if (!_context.LocalStore.TryLoadStagingMetadata(table, out var stagingMetadata))
                {
                    stagingMetadata = new StagingMetadata
                    {
                        LastTimestamp = tableMetadata.LastTimestamp,
                        LastIds = { tableMetadata.LastIds },
                    };
                }

                if (_context.Config.FetchIds == null && !TryFetchEntities(table, tableMetadata.Config, stagingMetadata))
                    return false;

                if (!TryFetchDetail(table, tableMetadata, stagingMetadata))
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
                metadata.LastIds);

            if (_context.Config.FetchLimit != null)
                entities = entities.Take(_context.Config.FetchLimit.Value);

            using var enumerator = new EntityEnumerator(entities);
            bool first = true;
            while (!enumerator.Finished)
            {
                var chunk = _context.LocalStore.WriteStagedEntities(table, enumerator.Take(chunkSize));
                if (chunk != null)
                {
                    metadata.LastTimestamp = enumerator.LastTimestamp;
                    metadata.LastIds.ReplaceWith(enumerator.LastIds);

                    if (config.DetailFields.Count > 0 && config.Detail != DetailBehavior.Skip)
                    {
                        metadata.FetchDetail.Remove(chunk); // in case we have same chunk as previously downloaded
                        metadata.FetchDetail.Add(chunk);
                    }
                    else
                    {
                        metadata.Merge.Remove(chunk); // in case we have same chunk as previously downloaded
                        metadata.Merge.Add(chunk);
                    }

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
                var chunk = metadata.FetchDetail[0];

                var entities = GetDetailForEntities(
                    table,
                    config,
                    _context.LocalStore.ReadStagedEntities(chunk));

                // TODO: chunk this into smaller files or create smaller fetchDetail chunks in the first place (change the chunkSize in TryFetchEntities)
                var detailChunk = _context.LocalStore.WriteStagedEntities(table, entities, detailForChunk: chunk);
                if (detailChunk != null)
                {
                    metadata.Merge.Remove(detailChunk);
                    metadata.Merge.Add(detailChunk);
                    Console.WriteLine($"Wrote {detailChunk} to staging");
                }

                metadata.FetchDetail.Remove(chunk);
                _context.LocalStore.SaveStagingMetadata(table, metadata);
                _context.LocalStore.RemoveStagedEntities(chunk);
            }

            if (_context.Config.FetchIds != null || config.Detail == DetailBehavior.Backfill)
            {
                var entities = _context.Config.FetchIds != null ?
                    _context.Config.FetchIds.Select(id => _context.RemoteStore.GetEntityDetail(table, config, id)) :
                    GetDetailForEntities(
                        table,
                        config,
                        GetEntitiesToBackfillDetail(table, tableMetadata, metadata.DetailLastId));

                using var enumerator = new EntityEnumerator(entities);
                while (!enumerator.Finished)
                {
                    var chunk = _context.LocalStore.WriteStagedEntities(
                        table,
                        enumerator.Take(chunkSize),
                        detailChunkById: true);

                    if (chunk == null)
                        continue;

                    if (config.Detail == DetailBehavior.Backfill)
                        metadata.DetailLastId = enumerator.LastId;

                    metadata.Merge.Remove(chunk);
                    metadata.Merge.Add(chunk);
                    _context.LocalStore.SaveStagingMetadata(table, metadata);

                    Console.WriteLine($"Wrote {chunk} to staging");
                }
            }

            return true;
        }

        private IEnumerable<TableEntity> GetDetailForEntities(string table, TableConfig config, IEnumerable<TableEntity> entities)
        {
            foreach (var entity in entities)
                yield return _context.RemoteStore.GetEntityDetail(table, config, entity);
        }

        private IEnumerable<TableEntity> GetEntitiesToBackfillDetail(string table, Metadata tableMetadata, long? detailLastId)
        {
            var detailFields = tableMetadata.Config.DetailFields;

            foreach (var entity in _context.LocalStore.ReadEntities(table, tableMetadata))
            {
                if (detailLastId != null && entity.Id <= detailLastId)
                    continue;

                if (entity.Properties.Keys.Intersect(detailFields).Count() != detailFields.Count)
                    yield return entity;
            }
        }
        private sealed class EntityEnumerator : IEnumerable<TableEntity>, IDisposable
        {
            private readonly IEnumerator<TableEntity> _entities;

            internal EntityEnumerator(IEnumerable<TableEntity> entities)
            {
                _entities = entities.GetEnumerator();
            }

            internal DateTime? LastTimestamp { get; private set; }
            internal HashSet<long> LastIds { get; } = new HashSet<long>();
            internal long? LastId { get; private set; }
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
                    if (entity.Timestamp != LastTimestamp)
                        LastIds.Clear();
                    LastIds.Add(entity.Id);
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
            // TODO: make this configurable, consider making it memory-based rather than count-based (after switching to System.Text.Json)
            const int batchSize = 10000;

            using var enumerator = new EntityEnumerator(_context, table, tableMetadata, stagingMetadata);
            while (!enumerator.Finished)
            {
                if (!_context.LocalStore.TryUpsertEntities(table, tableMetadata, enumerator.Take(batchSize)))
                    return false;

                enumerator.Flush();
            }

            return true;
        }

        private sealed class EntityEnumerator : IEnumerable<TableEntity>, IDisposable
        {
            private readonly FlowContext _context;
            private readonly string _table;
            private readonly Metadata _tableMetadata;
            private readonly StagingMetadata _stagingMetadata;
            private readonly List<string> _chunksToRemove;
            private readonly IEnumerator<TableEntity> _entities;

            internal EntityEnumerator(
                FlowContext context,
                string table,
                Metadata tableMetadata,
                StagingMetadata stagingMetadata)
            {
                _context = context;
                _table = table;
                _tableMetadata = tableMetadata;
                _stagingMetadata = stagingMetadata;
                _chunksToRemove = new List<string>();
                _entities = ReadEntitiesToMerge().GetEnumerator();
            }

            internal bool Finished { get; private set; }

            public void Dispose()
            {
                _entities.Dispose();
            }

            public void Flush()
            {
                if (_stagingMetadata.Merge.Count > 0)
                {
                    _context.LocalStore.SaveStagingMetadata(_table, _stagingMetadata);
                }
                else
                {
                    _tableMetadata.LastTimestamp = _stagingMetadata.LastTimestamp;
                    _tableMetadata.LastIds.ReplaceWith(_stagingMetadata.LastIds);
                    _context.LocalStore.SaveMetadata(_table, _tableMetadata);
                    _context.LocalStore.RemoveStagingMetadata(_table);
                }

                foreach (var chunk in _chunksToRemove)
                    _context.LocalStore.RemoveStagedEntities(chunk);

                _chunksToRemove.Clear();
            }

            public IEnumerator<TableEntity> GetEnumerator()
            {
                while (_entities.MoveNext())
                    yield return _entities.Current;

                Finished = true;
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            private IEnumerable<TableEntity> ReadEntitiesToMerge()
            {
                while (_stagingMetadata.Merge.Count > 0)
                {
                    var chunk = _stagingMetadata.Merge[0];

                    Console.WriteLine($"Merging {_table} from {chunk}... ");

                    var entities = _context.LocalStore.ReadStagedEntities(chunk);
                    foreach (var entity in entities)
                        yield return entity;

                    _stagingMetadata.Merge.Remove(chunk);
                    _chunksToRemove.Add(chunk);
                }
            }
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

    public sealed class PartitionFlow
    {
        private readonly FlowContext _context;
        private readonly string _table;
        private readonly IReadOnlyDictionary<string, IReadOnlyList<string>> _columnPartitions;

        public PartitionFlow(FlowContext context, string table, IReadOnlyDictionary<string, IReadOnlyList<string>> columnPartitions)
        {
            _context = context;
            _table = table;
            _columnPartitions = columnPartitions;
        }

        public bool TryExecute()
        {
            // TODO: share code with CompressFlow??
            if (!_context.LocalStore.TryLoadMetadata(_table, out var tableMetadata))
                return false;

            Console.WriteLine($"Partitioning {_table}... ");

            var newMetadata = tableMetadata.Clone();

            newMetadata.Config.ColumnPartitions.OverrideWith(_columnPartitions);

            // TODO: consider making this a first class method on ITableStore (CleanEntities?)
            var entities = _context.LocalStore.ReadEntities(_table, tableMetadata);
            _context.LocalStore.TryUpsertEntities(_table, newMetadata, entities);

            _context.LocalStore.SaveMetadata(_table, newMetadata);

            return true;
        }
    }
}
