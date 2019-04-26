using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace GiantBombDataTool
{
    public sealed class FlowContext
    {
        public FlowContext(
            LocalJsonTableStore localStore,
            GiantBombTableStore remoteStore,
            IReadOnlyList<string> tables,
            Config config)
        {
            LocalStore = localStore;
            RemoteStore = remoteStore;
            Tables = tables;
            Config = config;
        }

        public LocalJsonTableStore LocalStore { get; }
        public GiantBombTableStore RemoteStore { get; }
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

                var fetchFlow = new FetchFlow(_context.WithTables(new[] { table }));
                if (!fetchFlow.TryExecute())
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
                // TODO: check staging metadata first

                if (!_context.LocalStore.TryLoadMetadata(table, out var metadata))
                    return false;

                if (metadata.NextTimestamp == null)
                {
                    metadata.NextId ??= 0;
                    metadata.NextTimestamp = DateTime.UtcNow;
                }

                if (metadata.NextId != null)
                {
                    if (!TryFetchEntitiesById(table, metadata.Config, metadata.NextId.Value))
                        return false;
                }
            }

            return true;
        }

        private bool TryFetchEntitiesById(string table, TableConfig config, long nextId)
        {
            const int chunkSize = 1000;

            /*
             * Loop:
             *  download resource list from GiantBomb (returns array of JsonEntity and indicator for additional pages)
             *  write contents to staging store (resource, array of JsonEntity -- {resource}-{firstId}.json file)
             *  if additional pages then continue
             */
            var entities = _context.RemoteStore.GetEntitiesById(table, nextId, config);
            var enumerator = new EntityEnumerator(entities);
            var chunk = _context.LocalStore.WriteStagedEntities(table, enumerator.Take(chunkSize));
            if (chunk != null)
            {
                nextId = enumerator.LastId + 1;
                var metadata = new StagingMetadata
                {
                    NextId = nextId,
                    Merge = { chunk },
                };
                _context.LocalStore.SaveStagingMetadata(table, metadata);
            }
            return true;
        }

        private sealed class EntityEnumerator : IEnumerable<TableEntity>
        {
            private readonly IEnumerable<TableEntity> _entities;

            internal EntityEnumerator(IEnumerable<TableEntity> entities)
            {
                _entities = entities;
            }

            internal long LastId { get; private set; }

            public IEnumerator<TableEntity> GetEnumerator()
            {
                foreach (var entity in _entities)
                {
                    LastId = entity.Id;
                    yield return entity;
                }
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }
        }
    }
}
