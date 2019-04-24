using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;

namespace GiantBombDataTool
{
    [JsonObject(NamingStrategyType = typeof(CamelCaseNamingStrategy))]
    public abstract class MetadataBase
    {
        [JsonProperty(DefaultValueHandling = DefaultValueHandling.Ignore)]
        public long? NextId { get; set; }

        [JsonProperty(DefaultValueHandling = DefaultValueHandling.Ignore)]
        public DateTime? NextTimestamp { get; set; }
    }

    public sealed class Metadata : MetadataBase
    {
        [JsonProperty(Required = Required.Always)]
        public Config Config { get; set; }
    }

    public sealed class StagingMetadata : MetadataBase
    {
        public List<string> Merge { get; } = new List<string>();
    }
}
