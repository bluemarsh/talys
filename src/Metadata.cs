using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace GiantBombDataTool
{
    [JsonObject(NamingStrategyType = typeof(CamelCaseNamingStrategy))]
    public sealed class Metadata
    {
        [JsonProperty(Required = Required.Always)]
        public ResourceConfig Config { get; set; } = new ResourceConfig();

        [JsonProperty(DefaultValueHandling = DefaultValueHandling.Ignore)]
        public long? NextId { get; set; }

        [JsonProperty(DefaultValueHandling = DefaultValueHandling.Ignore)]
        public DateTime? NextTimestamp { get; set; }
    }
}
