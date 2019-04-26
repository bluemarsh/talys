using System;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;

namespace GiantBombDataTool
{
    public interface IReadOnlyTableStore
    {
        //StoreMetadata CreateMetadata(StoreConfig config);
        IEnumerable<TableEntity> GetEntitiesById(string table, long nextId, TableConfig config);
    }

    public interface ITableStore // TODO: derives IReadOnlyTableStore
    {
        object Location { get; } // TODO: move to IReadOnlyTableStore
    }
    
    public interface ITableMetadataStore
    {
        bool TryInitialize(string table, Metadata metadata);
        bool TryLoadMetadata(string table, out Metadata metadata);
        IEnumerable<string> GetTables();
    }

    public interface ITableStagingStore
    {
        bool TryLoadStagingMetadata(string table, out StagingMetadata metadata);
        void SaveStagingMetadata(string table, StagingMetadata metadata);
        string? WriteStagedEntities(string table, IEnumerable<TableEntity> entities);
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
