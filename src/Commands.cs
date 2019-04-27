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
        [Option('t', "tables", Required = false, Default = "*", HelpText = "Comma-delimited list of tables to clone, or * for all.")]
        public string Tables { get; set; } = string.Empty;

        [Option('a', "api-key", HelpText = "API key for GiantBomb.")]
        public string? ApiKey { get; set; }

        [Option('c', "chunk-size", HelpText = "Chunk size for staging files.")]
        public int? ChunkSize { get; set; }

        [Option("verbose", HelpText = "Enable verbose tracing output")]
        public bool Verbose { get; set; }

        public abstract int Execute();

        protected bool TryParseCommonArgs(string localStorePath, out FlowContext flowContext)
        {
            if (Tables.Length == 0) Tables = "*";
            if (localStorePath.Length == 0) localStorePath = ".";

            var tables = Tables == "*" ? Array.Empty<string>() :
                Tables.Split(',', StringSplitOptions.RemoveEmptyEntries).Select(s => s.Trim()).ToArray();

            var config = GetConfig();

            var remoteStore = CreateGiantBombStore(config);

            var localStore = new LocalJsonTableStore(Path.GetFullPath(localStorePath));

            flowContext = new FlowContext(localStore, remoteStore, tables, config);
            return true;
        }

        private Config GetConfig()
        {
            var config = JsonConvert.DeserializeObject<Config>(
                GetType().GetManifestResourceString("config.json"));

            if (!string.IsNullOrEmpty(ApiKey))
                config.ApiKey = ApiKey;

            if (ChunkSize != null)
                config.ChunkSize = ChunkSize;

            if (Verbose)
                config.Verbose = true;

            foreach (var tableConfig in config.Tables.Values)
                tableConfig.OverrideWith(config);

            return config;
        }

        private GiantBombTableStore CreateGiantBombStore(Config config)
        {
            string userAgent = CommandLine.Text.HeadingInfo.Default.ToString();
            return new GiantBombTableStore(userAgent, config.ApiKey, config.Verbose ?? false);
        }
    }

    [Verb("clone", HelpText = "Clone GiantBomb resources to a data store.")]
    internal sealed class CloneCommand : Command
    {
        //[Value(0, MetaName = "source", Required = false, Default = "", HelpText = "Source store to clone from.")]
        //public string Source { get; set; } = string.Empty;

        [Value(0, MetaName = "target", Required = false, Default = ".", HelpText = "Target store to clone to.")]
        public string Target { get; set; } = string.Empty;

        public override int Execute()
        {
            if (!TryParseCommonArgs(Target, out var flowContext))
                return 1;

            Console.WriteLine($"Cloning {Tables} to {flowContext.LocalStore.Location}");

            var flow = new CloneFlow(flowContext);
            return flow.TryExecute() ? 0 : 1;
        }
    }

    [Verb("fetch", HelpText = "Fetch latest GiantBomb resources without merging into the data store.")]
    internal sealed class FetchCommand : Command
    {
        [Option('s', "store", Required = false, Default = ".", HelpText = "Store location.")]
        public string Store { get; set; } = string.Empty;

        public override int Execute()
        {
            if (!TryParseCommonArgs(Store, out var flowContext))
                return 1;

            Console.WriteLine($"Fetching {Tables} to {flowContext.LocalStore.Location}");

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
