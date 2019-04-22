using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace GiantBombDataTool
{
    [JsonObject(NamingStrategyType = typeof(CamelCaseNamingStrategy))]
    public abstract class BaseConfig
    {
        [JsonProperty(DefaultValueHandling = DefaultValueHandling.Ignore)]
        public string? ApiKey { get; set; }

        [JsonProperty(DefaultValueHandling = DefaultValueHandling.Ignore)]
        public bool Verbose { get; set; }

        protected void OverrideWith(BaseConfig other)
        {
            if (!string.IsNullOrWhiteSpace(other.ApiKey))
                ApiKey = other.ApiKey;
        }
    }

    public class Config : BaseConfig
    {
        // TODO: StoreApiKey option (writes ApiKey to metadata)

        public Dictionary<string, ResourceConfig> Resources { get; } = new Dictionary<string, ResourceConfig>();

        public void OverrideWith(Config other)
        {
            base.OverrideWith(other);
        }
    }

    public class ResourceConfig : BaseConfig
    {
        [JsonProperty(Required = Required.Always)]
        public IReadOnlyList<string> Fields { get; set; } = Array.Empty<string>();
    }
}
