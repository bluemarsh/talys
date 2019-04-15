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

        protected void OverrideWith(BaseConfig other)
        {
            if (!string.IsNullOrWhiteSpace(other.ApiKey))
                ApiKey = other.ApiKey;
        }
    }

    public class Config : BaseConfig
    {
        public Dictionary<string, ResourceConfig> Resources { get; set; } = new Dictionary<string, ResourceConfig>();

        public void OverrideWith(Config other)
        {
            base.OverrideWith(other);
        }
    }

    public class ResourceConfig : BaseConfig
    {
        public string[]? Fields { get; set; }
    }
}
