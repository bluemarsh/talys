using System;
using System.Collections.Generic;
using System.IO;

namespace GiantBombDataTool
{
    public sealed class FlowContext
    {
        public FlowContext(
            LocalJsonTableStore localStore,
            GiantBombTableStore remoteStore,
            IEnumerable<string> tables,
            Config config)
        {
            LocalStore = localStore;
            RemoteStore = remoteStore;
            Tables = tables;
            Config = config;
        }

        public LocalJsonTableStore LocalStore { get; }
        public GiantBombTableStore RemoteStore { get; }
        public IEnumerable<string> Tables { get; }
        public StoreConfig Config { get; }

        public FlowContext WithTables(IEnumerable<string> tables)
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
            foreach (string table in _context.Tables)
            {
                var tableConfig = _context.Config.Tables[table];

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
            foreach (string table in _context.Tables)
            {
                if (!_context.LocalStore.TryGetMetadata(table, out var metadata))
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

        private bool TryFetchEntitiesById(string resource, Config config, long nextId)
        {
            /*
             * Loop:
             *  download resource list from GiantBomb (returns array of JsonEntity and indicator for additional pages)
             *  write contents to staging store (resource, array of JsonEntity -- {resource}-{firstId}.json file)
             *  if additional pages then continue
             */
            var entities = _context.RemoteStore.GetEntitiesById(nextId);
            _context.LocalStore.WriteStagedEntities(resource, entities);
            return true;
        }
    }
}
