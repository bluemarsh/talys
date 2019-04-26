using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace GiantBombDataTool
{
    [JsonObject(NamingStrategyType = typeof(CamelCaseNamingStrategy))]
    public abstract class CommonMetadata
    {
        [JsonProperty(DefaultValueHandling = DefaultValueHandling.Ignore)]
        public long? NextId { get; set; }

        [JsonProperty(DefaultValueHandling = DefaultValueHandling.Ignore)]
        public DateTime? NextTimestamp { get; set; }
    }

    public sealed class Metadata : CommonMetadata
    {
        [JsonProperty(Required = Required.Always)]
        public TableConfig Config { get; set; } = new TableConfig();
    }

    public sealed class StagingMetadata : CommonMetadata
    {
        public List<string> Merge { get; } = new List<string>();
    }
}
