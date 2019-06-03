using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace Talys
{
    [JsonObject(NamingStrategyType = typeof(CamelCaseNamingStrategy))]
    public abstract class CommonMetadata
    {
        [JsonProperty(DefaultValueHandling = DefaultValueHandling.Ignore, Order = 10)]
        public DateTime? LastTimestamp { get; set; }

        // TODO: included for back compat only (can be removed once all existing metadata has been updated)
        [JsonProperty(DefaultValueHandling = DefaultValueHandling.Ignore, Order = 20)]
        public long? LastId
        {
            get { return null; }
            set
            {
                if (value != null)
                    LastIds.Add(value.Value);
            }
        }

        [JsonProperty(DefaultValueHandling = DefaultValueHandling.Ignore, Order = 20)]
        public HashSet<long> LastIds { get; } = new HashSet<long>();

        public bool ShouldSerializeLastIds() => LastIds.Count > 0;
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
                LastIds = { LastIds },
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
