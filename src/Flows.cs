using System;
using System.Collections;
using System.Collections.Generic;
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

                var fetchFlow = new FetchFlow(tableContext);
                if (!fetchFlow.TryExecute())
                    return false;

                var mergeFlow = new MergeFlow(tableContext);
                if (!mergeFlow.TryExecute())
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
            }

            return true;
        }

        private bool TryFetchEntities(string table, TableConfig config, StagingMetadata metadata)
        {
            int chunkSize = config.ChunkSize ?? CommonConfig.DefaultChunkSize;

            string lastUpdateText = metadata.LastTimestamp != null ?
                metadata.LastTimestamp.Value.ToString("yyyy-MM-dd HH:mm:ss") :
                "forever";
            Console.WriteLine($"Retrieving {table} updated since {lastUpdateText}");

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
                    metadata.Merge.Add(chunk);
                    _context.LocalStore.SaveStagingMetadata(table, metadata);

                    Console.WriteLine($"Wrote {chunk} to staging");
                }
                else if (first)
                {
                    Console.WriteLine("Already up to date.");
                }
                first = false;
            }
            return true;
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
                var chunk = stagingMetadata.Merge[0];

                Console.WriteLine($"Merging {table} from {chunk}");

                var entities = _context.LocalStore.ReadStagedEntities(chunk);
                if (!_context.LocalStore.TryUpsertEntities(table, tableMetadata, entities))
                    return false;

                stagingMetadata.Merge.RemoveAt(0);
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
}
