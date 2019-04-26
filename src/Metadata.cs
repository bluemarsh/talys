using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace GiantBombDataTool
{
    [JsonObject(NamingStrategyType = typeof(CamelCaseNamingStrategy))]
    public abstract class CommonMetadata
    {
        [JsonProperty(DefaultValueHandling = DefaultValueHandling.Ignore, Order = 10)]
        public DateTime? LastTimestamp { get; set; }

        [JsonProperty(DefaultValueHandling = DefaultValueHandling.Ignore, Order = 20)]
        public long? LastId { get; set; }
    }

    public sealed class Metadata : CommonMetadata
    {
        [JsonProperty(Required = Required.Always)]
        public TableConfig Config { get; set; } = new TableConfig();
    }

    public sealed class StagingMetadata : CommonMetadata
    {
        [JsonProperty(Order = 100)]
        public List<string> Merge { get; } = new List<string>();
    }
}
