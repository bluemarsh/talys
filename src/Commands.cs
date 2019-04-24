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
        [Option('g', "staging", Required = false, HelpText = "Staging store for fetched data.")]
        public string Staging { get; set; } = string.Empty;

        [Option('t', "tables", Required = false, Default = "*", HelpText = "Comma-delimited list of tables to clone, or * for all.")]
        public string Tables { get; set; } = string.Empty;

        [Option("verbose", HelpText = "Enable verbose tracing output")]
        public bool Verbose { get; set; }

        public abstract int Execute();

        protected bool TryGetStoreConfig(string? embeddedConfig = null, string? configPath = null)
        {
        }

        protected bool TryParseCommonArgs(out FlowContext flowContext)
        {
            if (Resources.Length == 0) Resources = "*";
            if (TargetPath.Length == 0) TargetPath = ".";

            var config = GetConfig();
            if (!TryParseResources(Resources, config, out var resourceKeys))
            {
                flowContext = null!;
                return false;
            }

            var sourceStore = CreateGiantBombStore(config);

            string targetPath = Path.GetFullPath(TargetPath);
            var targetStore = new LocalJsonTableStore(targetPath);

            flowContext = new FlowContext(sourceStore, targetStore, targetStore, resourceKeys, config);
            return true;
        }

        private GiantBombTableStore CreateGiantBombStore(Config config)
        {
            if (config.ApiKey is null)
                throw new InvalidOperationException($"Missing {nameof(config.ApiKey)} on configuration");

            string userAgent = CommandLine.Text.HeadingInfo.Default.ToString();
            return new GiantBombTableStore(config.ApiKey, userAgent, config.Verbose);
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
        [Value(0, MetaName = "source", Required = false, Default = "", HelpText = "Source store to clone from.")]
        public string Source { get; set; } = string.Empty;

        [Value(1, MetaName = "target", Required = false, Default = ".", HelpText = "Target store to clone to.")]
        public string Target { get; set; } = string.Empty;

        [Option('a', "api-key", HelpText = "API key for source store.")]
        public string? ApiKey { get; set; }

        public override int Execute()
        {
            if (!TryParseCommonArgs(out var flowContext))
                return 1;

            Console.WriteLine($"Cloning {Tables} to {flowContext.TargetStore.Location}");

            var flow = new CloneFlow(flowContext);
            return flow.TryExecute() ? 0 : 1;
        }
    }

    [Verb("fetch", HelpText = "Fetch latest GiantBomb resources without merging into the data store.")]
    internal sealed class FetchCommand : Command
    {
        // TODO: we need to instead point to the metadata store
        [Option('s', "store", Required = false, Default = ".", HelpText = "Metadata store location.")]
        public string Store { get; set; } = string.Empty;

        public override int Execute()
        {
            if (!TryParseCommonArgs(out var flowContext))
                return 1;

            Console.WriteLine($"Fetching {Tables} to {flowContext.TargetStore.Location}");

            var flow = new FetchFlow(flowContext);
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
