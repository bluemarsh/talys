using System;
using System.Collections.Generic;
using System.IO;

namespace GiantBombDataTool
{
    public sealed class CloneFlow
    {
        private readonly IJsonDataStore _store;
        private readonly IJsonStagingStore _stagingStore;
        private readonly IEnumerable<string> _resources;
        private readonly Config _config;

        public CloneFlow(IJsonDataStore store, IJsonStagingStore stagingStore, IEnumerable<string> resources, Config config)
        {
            _store = store;
            _stagingStore = stagingStore;
            _resources = resources;
            _config = config;
        }

        public bool TryExecute()
        {
            foreach (string resource in _resources)
            {
                var resourceConfig = _config.Resources[resource];

                var metadata = new Metadata
                {
                    Config = resourceConfig,
                };

                if (!_store.TryInitialize(resource, metadata))
                    return false;

                var fetchFlow = new FetchFlow(_store, _stagingStore, new[] { resource }, _config);
                if (!fetchFlow.TryExecute())
                    return false;
            }

            return true;
        }
    }

    public sealed class FetchFlow
    {
        private readonly IJsonDataStore _store;
        private readonly IJsonStagingStore _stagingStore;
        private readonly IEnumerable<string> _resources;
        private readonly Config _config;

        public FetchFlow(IJsonDataStore store, IJsonStagingStore stagingStore, IEnumerable<string> resources, Config config)
        {
            if (config.ApiKey is null)
                throw new ArgumentException($"Missing {nameof(config.ApiKey)} on {nameof(config)}");

            _store = store;
            _stagingStore = stagingStore;
            _resources = resources;
            _config = config;
        }

        public bool TryExecute()
        {
            foreach (string resource in _resources)
            {
                if (!_store.TryGetMetadata(resource, out var metadata))
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
            _stagingStore.WriteStagedEntities(resource, new[] { new JsonEntity(nextId, DateTime.UtcNow, obj) });
            return true;
        }

        private GiantBombApiClient CreateGiantBombClient()
        {
            if (_config.ApiKey is null)
                throw new InvalidOperationException($"Missing {nameof(_config.ApiKey)} on configuration");

            string userAgent = CommandLine.Text.HeadingInfo.Default.ToString();
            return new GiantBombApiClient(_config.ApiKey, userAgent, _config.Verbose);
        }
    }
}
