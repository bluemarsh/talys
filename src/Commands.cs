using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using CommandLine;
using GiantBombDataTool.Stores;
using Newtonsoft.Json;

namespace GiantBombDataTool
{
    internal abstract class Command
    {
        [Option('t', "tables", Required = false, Default = "*", HelpText = "Comma-delimited list of tables to clone, or * for all.")]
        public string Tables { get; set; } = string.Empty;

        [Option('a', "api-key", HelpText = "API key for GiantBomb.")]
        public string? ApiKey { get; set; }

        [Option('c', "compression", HelpText = "Compression to use for local store.")]
        public TableCompressionKind? Compression { get; set; }

        [Option("chunk-size", HelpText = "Chunk size for staging files.")]
        public int? ChunkSize { get; set; }

        [Option("detail", HelpText = "Fetch detail behavior.")]
        public DetailBehavior? Detail { get; set; }

        [Option("fetch-limit", HelpText = "Limit number of entities to fetch per resource.")]
        public int? FetchLimit { get; set; }

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

            if (Compression != null)
                config.Compression = Compression;

            if (ChunkSize != null)
                config.ChunkSize = ChunkSize;

            if (Detail != null)
                config.Detail = Detail;

            if (FetchLimit != null)
                config.FetchLimit = FetchLimit;

            if (Verbose)
                config.Verbose = true;

            foreach (var tableConfig in config.Tables.Values)
                tableConfig.OverrideWith(config);

            return config;
        }

        private IReadOnlyTableStore CreateGiantBombStore(Config config)
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

    internal abstract class LocalStoreCommand : Command
    {
        [Option('s', "store", Required = false, Default = ".", HelpText = "Store location.")]
        public string Store { get; set; } = string.Empty;
    }

    [Verb("fetch", HelpText = "Fetch latest changes without merging into them into the local store.")]
    internal sealed class FetchCommand : LocalStoreCommand
    {
        public override int Execute()
        {
            if (!TryParseCommonArgs(Store, out var flowContext))
                return 1;

            Console.WriteLine($"Fetching {Tables} to {flowContext.LocalStore.Location}");

            var flow = new FetchFlow(flowContext);
            return flow.TryExecute() ? 0 : 1;
        }
    }

    [Verb("merge", HelpText = "Merge resources that were previously fetched into the local store.")]
    internal sealed class MergeCommand : LocalStoreCommand
    {
        public override int Execute()
        {
            if (!TryParseCommonArgs(Store, out var flowContext))
                return 1;

            Console.WriteLine($"Merging {Tables} to {flowContext.LocalStore.Location}");

            var flow = new MergeFlow(flowContext);
            return flow.TryExecute() ? 0 : 1;
        }
    }

    [Verb("pull", HelpText = "Fetch latest changes and merge them into the local store.")]
    internal sealed class PullCommand : LocalStoreCommand
    {
        public override int Execute()
        {
            if (!TryParseCommonArgs(Store, out var flowContext))
                return 1;

            Console.WriteLine($"Fetching {Tables} and merging to {flowContext.LocalStore.Location}");

            var flow = new PullFlow(flowContext);
            return flow.TryExecute() ? 0 : 1;
        }
    }

    [Verb("compress",HelpText = "Compresses a currently-uncompressed local store.")]
    internal sealed class CompressCommand : LocalStoreCommand
    {
        public override int Execute()
        {
            if (!TryParseCommonArgs(Store, out var flowContext))
                return 1;

            Console.WriteLine($"Compressing {Tables} in {flowContext.LocalStore.Location}");

            var flow = new CompressFlow(flowContext, CompressionMode.Compress);
            return flow.TryExecute() ? 0 : 1;
        }
    }

    [Verb("decompress",HelpText = "Decompresses a currently-compressed local store.")]
    internal sealed class DecompressCommand : LocalStoreCommand
    {
        public override int Execute()
        {
            if (!TryParseCommonArgs(Store, out var flowContext))
                return 1;

            Console.WriteLine($"Decompressing {Tables} in {flowContext.LocalStore.Location}");

            var flow = new CompressFlow(flowContext, CompressionMode.Decompress);
            return flow.TryExecute() ? 0 : 1;
        }
    }

    // TODO: additional commands
    // - clean (download id-only list, remove entities that are no longer present)
    // - status (print last timestamps for each table in local store? and last clean time?)
}
