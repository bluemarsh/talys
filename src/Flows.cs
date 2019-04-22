using System;
using System.Collections.Generic;
using System.IO;

namespace GiantBombDataTool
{
    public sealed class FlowContext
    {
        public FlowContext(
            ITableStore store,
            ITableStagingStore stagingStore,
            IEnumerable<string> resources,
            Config config)
        {
            Store = store;
            StagingStore = stagingStore;
            Resources = resources;
            Config = config;
        }

        public ITableStore Store { get; }
        public ITableStagingStore StagingStore { get; }
        public IEnumerable<string> Resources { get; }
        public Config Config { get; }

        public FlowContext WithResources(IEnumerable<string> resources)
        {
            return new FlowContext(Store, StagingStore, resources, Config);
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
            foreach (string resource in _context.Resources)
            {
                var resourceConfig = _context.Config.Resources[resource];

                var metadata = new Metadata
                {
                    Config = resourceConfig,
                };

                if (!_context.Store.TryInitialize(resource, metadata))
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
            if (context.Config.ApiKey is null)
                throw new ArgumentException($"Missing {nameof(Config.ApiKey)} on {nameof(context.Config)}");

            _context = context;
        }

        public bool TryExecute()
        {
            foreach (string resource in _context.Resources)
            {
                if (!_context.Store.TryGetMetadata(resource, out var metadata))
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
            var giantBombClient = CreateGiantBombClient();
            var obj = giantBombClient.DownloadResourceList(
                resource,
                string.Join(",", resourceConfig.Fields),
                offset: 0,
                limit: 100,
                sort: "id:asc");
            _context.StagingStore.WriteStagedEntities(resource, new[] { new TableEntity(nextId, DateTime.UtcNow, obj) });
            return true;
        }

        private GiantBombApiClient CreateGiantBombClient()
        {
            if (_context.Config.ApiKey is null)
                throw new InvalidOperationException($"Missing {nameof(Config.ApiKey)} on configuration");

            string userAgent = CommandLine.Text.HeadingInfo.Default.ToString();
            return new GiantBombApiClient(_context.Config.ApiKey, userAgent, _context.Config.Verbose);
        }
    }
}
