using System;
using System.Collections.Generic;

namespace GiantBombDataTool
{
    public interface IReadOnlyTableStore
    {
        IEnumerable<TableEntity> GetEntitiesById(long nextId);
    }

    public interface ITableStore // TODO: derives IReadOnlyTableStore
    {
        object Location { get; }

        bool TryInitialize(string resource, Metadata metadata);
        bool TryGetMetadata(string resource, out Metadata metadata);
    }

    public interface ITableStagingStore
    {
        void WriteStagedEntities(string resource, IEnumerable<TableEntity> entities);
    }

    public sealed class TableEntity
    {
        public TableEntity(long id, DateTime timestamp, IReadOnlyDictionary<string, object> properties)
        {
            Id = id;
            Timestamp = timestamp;
            Properties = properties;
        }

        public long Id { get; }
        public DateTime Timestamp { get; }
        public IReadOnlyDictionary<string, object> Properties { get; }
    }
}
