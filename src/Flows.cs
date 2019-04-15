using System;
using System.Collections.Generic;

namespace GiantBombDataTool
{
    public sealed class CloneFlow
    {
        private readonly IJsonDataStoreFactory _storeFactory;
        private readonly IEnumerable<string> _resources;
        private readonly Config _config;

        public CloneFlow(IJsonDataStoreFactory storeFactory, IEnumerable<string> resources, Config config)
        {
            _storeFactory = storeFactory;
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
                    NextId = 1,
                    NextTimestamp = DateTime.UtcNow,
                };

                if (!_storeFactory.TryCreate(resource, metadata, out var store))
                    return false;
            }

            return true;
        }
    }
}
