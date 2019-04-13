using System;
using System.IO;
using CommandLine;
using Newtonsoft.Json;

namespace GiantBombDataTool
{
    interface ICommand
    {
        int Execute();
    }

    [Verb("clone", HelpText = "Clone GiantBomb resources to a data store.")]
    class CloneCommand : ICommand
    {
        [Value(0, MetaName = "resources", Required = true, HelpText = "Comma-delimited list of resources to clone, or * for all.")]
        public string Resources { get; set; }

        [Value(1, MetaName = "target-path", Required = true, HelpText = "Target path for the data store.")]
        public string TargetPath { get; set; }

        public int Execute()
        {
            string storePath = Path.GetFullPath(TargetPath);
            Console.WriteLine($"Cloning {Resources} to {storePath}");

            Directory.CreateDirectory(storePath);

            string resource = Resources;
            string metadataPath = Path.Combine(storePath, $"{resource}.metadata.json");

            if (File.Exists(metadataPath) || Directory.Exists(metadataPath))
            {
                Console.WriteLine($"Target data store already exists: {metadataPath}");
                return 1;
            }

            var metadata = new Metadata
            {
                Config = new ResourceConfig(),
                NextId = 1,
                NextTimestamp = DateTime.UtcNow,
            };

            File.WriteAllText(metadataPath, JsonConvert.SerializeObject(metadata));

            Console.WriteLine($"Created data store: {metadataPath}");

            return 0;
        }
    }

    [Verb("fetch", HelpText = "Fetch latest GiantBomb resources without merging into the data store.")]
    class FetchCommand : ICommand
    {
        public int Execute()
        {
            Console.WriteLine("Execute Fetch");
            return 0;
        }
    }

    [Verb("merge", HelpText = "Merge resources that were previously fetched into the data store.")]
    class MergeCommand : ICommand
    {
        public int Execute()
        {
            Console.WriteLine("Execute Merge");
            return 0;
        }
    }
}
