using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace GiantBombDataTool
{
    [JsonObject(NamingStrategyType = typeof(CamelCaseNamingStrategy))]
    public class CommonConfig
    {
        [JsonProperty(DefaultValueHandling = DefaultValueHandling.Ignore)]
        public string? ApiKey { get; set; }
    }

    public class Config : CommonConfig
    {
        public Dictionary<string, TableConfig> Tables { get; } = new Dictionary<string, TableConfig>();

        [JsonProperty(DefaultValueHandling = DefaultValueHandling.Ignore)]
        public bool? Verbose { get; set; }

        public bool ShouldSerializeTables() => Tables.Count > 0;
    }

    public class TableConfig : CommonConfig
    {
        public IReadOnlyList<string> Fields { get; set; } = Array.Empty<string>();

        public bool ShouldSerializeFields() => Fields.Count > 0;
    }
}
