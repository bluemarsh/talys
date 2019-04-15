using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace GiantBombDataTool
{
    [JsonObject(NamingStrategyType = typeof(CamelCaseNamingStrategy))]
    public sealed class Metadata
    {
        public ResourceConfig? Config { get; set; }
        public int NextId { get; set; }
        public DateTime NextTimestamp { get; set; }
    }
}
