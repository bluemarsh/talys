using System;
using System.Collections.Generic;
using System.IO;

namespace GiantBombDataTool
{
    public sealed class FlowContext
    {
        public FlowContext(
            IReadOnlyTableStore sourceStore,
            ITableStore targetStore,
            ITableStagingStore stagingStore,
            IEnumerable<string> resources,
            Config config)
        {
            SourceStore = sourceStore;
            TargetStore = targetStore;
            StagingStore = stagingStore;
            Resources = resources;
            Config = config;
        }

        public IReadOnlyTableStore SourceStore { get; }
        public ITableStore TargetStore { get; }
        public ITableStagingStore StagingStore { get; }
        public IEnumerable<string> Resources { get; }
        public Config Config { get; }

        public FlowContext WithResources(IEnumerable<string> resources)
        {
            return new FlowContext(SourceStore, TargetStore, StagingStore, resources, Config);
        }
    }

    public sealed class CloneFlow
    {
        private readonly IReadOnlyTableStore _sourceStore;
        private readonly ITableStore _targetStore;
        private readonly ITableMetadataStore _metadataStore;
        private readonly ITableStagingStore _stagingStore;
        private readonly IEnumerable<string> _tables;
        private readonly StoreConfig _sourceConfig;
        private readonly StoreConfig _targetConfig;

        public CloneFlow(
            IReadOnlyTableStore sourceStore,
            ITableStore targetStore,
            ITableMetadataStore metadataStore,
            ITableStagingStore stagingStore,
            IEnumerable<string> tables,
            StoreConfig sourceConfig,
            StoreConfig targetConfig)
        {
            _sourceStore = sourceStore;
            _targetStore = targetStore;
            _metadataStore = metadataStore;
            _stagingStore = stagingStore;
            _tables = tables;
            _sourceConfig = sourceConfig;
            _targetConfig = targetConfig;
        }

        public bool TryExecute()
        {
            foreach (string table in _tables)
            {
                var resourceConfig = _context.Config.Resources[resource];

                var metadata = new Metadata
                {
                    Config = resourceConfig,
                };

                if (!_context.TargetStore.TryInitialize(resource, metadata))
                    return false;

                var fetchFlow = new FetchFlow(_context.WithResources(new[] { resource }));
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
            foreach (string resource in _context.Resources)
            {
                if (!_context.TargetStore.TryGetMetadata(resource, out var metadata))
                    return false;

                if (metadata.NextTimestamp == null)
                {
                    metadata.NextId ??= 0;
                    metadata.NextTimestamp = DateTime.UtcNow;
                }

                if (metadata.NextId != null)
                {
                    if (!TryFetchResourcesById(resource, metadata.Config, metadata.NextId.Value))
                        return false;
                }
            }

            return true;
        }

        private bool TryFetchResourcesById(string resource, ResourceConfig resourceConfig, long nextId)
        {
            /*
             * Loop:
             *  download resource list from GiantBomb (returns array of JsonEntity and indicator for additional pages)
             *  write contents to staging store (resource, array of JsonEntity -- {resource}-{firstId}.json file)
             *  if additional pages then continue
             */
            var entities = _context.SourceStore.GetEntitiesById(nextId);
            _context.StagingStore.WriteStagedEntities(resource, entities);
            return true;
        }
    }
}
