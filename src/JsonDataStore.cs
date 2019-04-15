using System;
using System.Collections.Generic;
using System.IO;
using Newtonsoft.Json;

namespace GiantBombDataTool
{
    public interface IJsonDataStoreFactory
    {
        bool TryCreate(string resource, Metadata metadata, out IJsonDataStore store);
    }

    public interface IJsonDataStore
    {
    }

    public sealed class LocalJsonDataStoreFactory : IJsonDataStoreFactory
    {
        private static readonly JsonSerializerSettings _serializerSettings = new JsonSerializerSettings
        {
            Formatting = Formatting.Indented,
        };

        private readonly string _storePath;

        public LocalJsonDataStoreFactory(string storePath)
        {
            _storePath = storePath;
        }

        public bool TryCreate(string resource, Metadata metadata, out IJsonDataStore store)
        {
            Directory.CreateDirectory(_storePath);

            string metadataPath = Path.Combine(_storePath, $"{resource}.metadata.json");

            if (File.Exists(metadataPath) || Directory.Exists(metadataPath))
            {
                Console.WriteLine($"Data store already exists: {metadataPath}");
                store = null!;
                return false;
            }

            File.WriteAllText(
                metadataPath,
                JsonConvert.SerializeObject(metadata, _serializerSettings));

            Console.WriteLine($"Created data store: {metadataPath}");

            store = new LocalJsonDataStore();
            return true;
        }
    }

    public sealed class LocalJsonDataStore : IJsonDataStore
    {
    }
}
