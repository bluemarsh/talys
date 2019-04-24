using System;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;

namespace GiantBombDataTool
{
    public interface IReadOnlyTableStore
    {
        StoreMetadata CreateMetadata(StoreConfig config);
        //void SetConfiguration(JObject configuration);
        IEnumerable<TableEntity> GetEntitiesById(long nextId);
    }

    public interface ITableStore // TODO: derives IReadOnlyTableStore
    {
        object Location { get; } // TODO: move to IReadOnlyTableStore
    }
    
    public interface ITableMetadataStore
    {
        bool TryInitialize(string table, Metadata metadata);
        bool TryGetMetadata(string table, out Metadata metadata);
    }

    public interface ITableStagingStore
    {
        bool TryGetStagingMetadata(string table, out StagingMetadata metadata);
        void WriteStagedEntities(string table, IEnumerable<TableEntity> entities);
    }

    public sealed class TableEntity
    {
        public TableEntity(long id, DateTime timestamp, JObject content)
        {
            Id = id;
            Timestamp = timestamp;
            Content = content;
        }

        public long Id { get; }
        public DateTime Timestamp { get; }
        public JObject Content { get; }
    }
}
