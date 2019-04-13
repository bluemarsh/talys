using System;
using System.Collections.Generic;
using System.Text;

namespace GiantBombDataTool
{
    class Config
    {
        public string ApiKey { get; set; }

        public Dictionary<string, ResourceConfig> Resources { get; set; }
    }

    class ResourceConfig
    {
        public string Fields { get; set; }
    }
}
