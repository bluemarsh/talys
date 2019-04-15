using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using CommandLine;
using Newtonsoft.Json;

namespace GiantBombDataTool
{
    internal abstract class Command
    {
        [Option('a', "api-key", HelpText = "API key for querying GiantBomb")]
        public string? ApiKey { get; set; }

        public abstract int Execute();

        protected Config GetConfig()
        {
            var config = JsonConvert.DeserializeObject<Config>(
                GetType().GetManifestResourceString("config.json"));

            // TODO: check for .giantbombconfig in working directory, user profile, and executable directory
            // also allow specifying config file as command line argument

            var argConfig = new Config
            {
                ApiKey = ApiKey,
            };
            config.OverrideWith(argConfig);

            return config;
        }

        protected bool TryParseResources(string resourcesArg, Config config, out IReadOnlyList<string> resourceKeys)
        {
            if (resourcesArg == "*")
            {
                resourceKeys = config.Resources.Keys.ToList();
                return true;
            }

            var resourceList = new List<string>();
            foreach (string s in resourcesArg.Split(','))
            {
                if (!config.Resources.ContainsKey(s))
                {
                    Console.WriteLine($"Unknown resource: {s}");
                    resourceKeys = null!;
                    return false;
                }
                resourceList.Add(s);
            }

            resourceKeys = resourceList;
            return true;
        }
    }

    [Verb("clone", HelpText = "Clone GiantBomb resources to a data store.")]
    internal sealed class CloneCommand : Command
    {
        [Value(0, MetaName = "resources", Required = true, HelpText = "Comma-delimited list of resources to clone, or * for all.")]
        public string Resources { get; set; } = string.Empty;

        [Value(1, MetaName = "target-path", Required = true, HelpText = "Target path for the data store.")]
        public string TargetPath { get; set; } = string.Empty;

        public override int Execute()
        {
            string storePath = Path.GetFullPath(TargetPath);

            var config = GetConfig();
            if (!TryParseResources(Resources, config, out var resources))
                return 1;

            Console.WriteLine($"Cloning {Resources} to {storePath}");

            var storeFactory = new LocalJsonDataStoreFactory(storePath);
            var flow = new CloneFlow(storeFactory, resources, config);
            return flow.TryExecute() ? 0 : 1;
        }
    }

    [Verb("fetch", HelpText = "Fetch latest GiantBomb resources without merging into the data store.")]
    internal sealed class FetchCommand : Command
    {
        public override int Execute()
        {
            Console.WriteLine("Execute Fetch");
            return 0;
        }
    }

    [Verb("merge", HelpText = "Merge resources that were previously fetched into the data store.")]
    internal sealed class MergeCommand : Command
    {
        public override int Execute()
        {
            Console.WriteLine("Execute Merge");
            return 0;
        }
    }
}
