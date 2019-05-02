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

        public Metadata Clone()
        {
            return new Metadata
            {
                Config = Config.Clone(),
                LastTimestamp = LastTimestamp,
                LastId = LastId,
            };
        }
    }

    public sealed class StagingMetadata : CommonMetadata
    {
        [JsonProperty(DefaultValueHandling = DefaultValueHandling.Ignore, Order = 100)]
        public long? DetailLastId { get; set; }

        [JsonProperty(Order = 110)]
        public List<string> FetchDetail { get; } = new List<string>();

        [JsonProperty(Order = 120)]
        public List<string> Merge { get; } = new List<string>();

        public bool ShouldSerializeFetchDetail() => FetchDetail.Count > 0;
        public bool ShouldSerializeMerge() => Merge.Count > 0;
    }
}
