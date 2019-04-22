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
        [Value(0, MetaName = "resources", Required = false, Default = "*", HelpText = "Comma-delimited list of resources to clone, or * for all.")]
        public string Resources { get; set; } = string.Empty;

        [Value(1, MetaName = "target-path", Required = false, Default = ".", HelpText = "Target path for the data store.")]
        public string TargetPath { get; set; } = string.Empty;

        [Option('a', "api-key", HelpText = "API key for querying GiantBomb")]
        public string? ApiKey { get; set; }

        [Option("verbose", HelpText = "Enable verbose tracing output")]
        public bool Verbose { get; set; }

        public abstract int Execute();

        protected bool TryParseCommonArgs(
            out Config config,
            out IReadOnlyList<string> resourceKeys,
            out IJsonDataStore store,
            out IJsonStagingStore stagingStore)
        {
            if (Resources.Length == 0) Resources = "*";
            if (TargetPath.Length == 0) TargetPath = ".";

            string storePath = Path.GetFullPath(TargetPath);
            var localStore = new LocalJsonDataStore(storePath);
            store = localStore;
            stagingStore = localStore;

            config = GetConfig();
            return TryParseResources(Resources, config, out resourceKeys);
        }

        private Config GetConfig()
        {
            var config = JsonConvert.DeserializeObject<Config>(
                GetType().GetManifestResourceString("config.json"));

            // TODO: check for .giantbombconfig in working directory, user profile, and executable directory
            // also allow specifying config file as command line argument

            var argConfig = new Config
            {
                ApiKey = ApiKey,
                Verbose = Verbose,
            };
            config.OverrideWith(argConfig);

            return config;
        }

        private bool TryParseResources(string resourcesArg, Config config, out IReadOnlyList<string> resourceKeys)
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
        public override int Execute()
        {
            if (!TryParseCommonArgs(out var config, out var resources, out var store, out var stagingStore))
                return 1;

            Console.WriteLine($"Cloning {Resources} to {store.Location}");

            var flow = new CloneFlow(store, stagingStore, resources, config);
            return flow.TryExecute() ? 0 : 1;
        }
    }

    [Verb("fetch", HelpText = "Fetch latest GiantBomb resources without merging into the data store.")]
    internal sealed class FetchCommand : Command
    {
        public override int Execute()
        {
            if (!TryParseCommonArgs(out var config, out var resources, out var store, out var stagingStore))
                return 1;

            Console.WriteLine($"Fetching {Resources} to {store.Location}");

            var flow = new FetchFlow(store, stagingStore, resources, config);
            return flow.TryExecute() ? 0 : 1;
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
