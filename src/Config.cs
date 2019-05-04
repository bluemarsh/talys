using System;
using System.Collections.Generic;
using GiantBombDataTool.Stores;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Serialization;

namespace GiantBombDataTool
{
    public enum DetailBehavior
    {
        Default,
        Backfill,
        Skip,
    }

    [JsonObject(NamingStrategyType = typeof(CamelCaseNamingStrategy))]
    public class CommonConfig
    {
        public const int DefaultChunkSize = 1000;
        public const int DefaultDetailChunkSize = 100;

        [JsonProperty(DefaultValueHandling = DefaultValueHandling.Ignore)]
        public string? ApiKey { get; set; }

        [JsonProperty(DefaultValueHandling = DefaultValueHandling.Ignore)]
        [JsonConverter(typeof(StringEnumConverter))]
        public TableCompressionKind? Compression { get; set; }

        [JsonProperty(DefaultValueHandling = DefaultValueHandling.Ignore)]
        public int? ChunkSize { get; set; }

        [JsonProperty(DefaultValueHandling = DefaultValueHandling.Ignore)]
        public DetailBehavior? Detail { get; set; }

        public void OverrideWith(CommonConfig other)
        {
            if (other.ApiKey != null)
                ApiKey = other.ApiKey;

            if (other.Compression != null)
                Compression = other.Compression;

            if (other.ChunkSize != null)
                ChunkSize = other.ChunkSize;

            if (other.Detail != null)
                Detail = other.Detail;
        }
    }

    public class Config : CommonConfig
    {
        public Dictionary<string, TableConfig> Tables { get; } = new Dictionary<string, TableConfig>();

        [JsonProperty(DefaultValueHandling = DefaultValueHandling.Ignore)]
        public int? FetchLimit { get; set; }

        [JsonProperty(DefaultValueHandling = DefaultValueHandling.Ignore)]
        public IReadOnlyList<long>? FetchIds { get; set; }

        [JsonProperty(DefaultValueHandling = DefaultValueHandling.Ignore)]
        public bool? Verbose { get; set; }

        public bool ShouldSerializeTables() => Tables.Count > 0;
    }

    public class TableConfig : CommonConfig
    {
        public long Id { get; set; }

        [JsonProperty(DefaultValueHandling = DefaultValueHandling.Ignore)]
        public string? DetailName { get; set; }

        public IReadOnlyList<string> Fields { get; set; } = Array.Empty<string>();

        public IReadOnlyList<string> DetailFields { get; set; } = Array.Empty<string>();

        public bool ShouldSerializeFields() => Fields.Count > 0;
        public bool ShouldSerializeDetailFields() => DetailFields.Count > 0;

        public TableConfig Clone()
        {
            var clone = new TableConfig
            {
                Fields = Fields,
            };
            clone.OverrideWith(this);
            return clone;
        }
    }
}
