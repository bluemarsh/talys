using System;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;

namespace Talys
{
    public interface IReadOnlyTableStore
    {
        IEnumerable<TableEntity> GetEntitiesByTimestamp(
            string table,
            TableConfig config,
            DateTime? lastTimestamp,
            IEnumerable<long> lastIds);
        TableEntity GetEntityDetail(string table, TableConfig config, long id);
        TableEntity GetEntityDetail(string table, TableConfig config, TableEntity entity);
    }

    public interface ITableStore // TODO: derives IReadOnlyTableStore
    {
        object Location { get; } // TODO: move to IReadOnlyTableStore

        bool TryUpsertEntities(string table, Metadata metadata, IEnumerable<TableEntity> entities);
        IEnumerable<TableEntity> ReadEntities(string table, Metadata metadata);
    }
    
    public interface ITableMetadataStore
    {
        IEnumerable<string> GetTables();
        bool TryInitialize(string table, Metadata metadata);
        bool TryLoadMetadata(string table, out Metadata metadata);
        void SaveMetadata(string table, Metadata metadata);
    }

    public interface ITableStagingStore
    {
        bool TryLoadStagingMetadata(string table, out StagingMetadata metadata);
        void SaveStagingMetadata(string table, StagingMetadata metadata);
        void RemoveStagingMetadata(string table);
        string? WriteStagedEntities(
            string table,
            IEnumerable<TableEntity> entities,
            bool detailChunkById = false,
            string? detailForChunk = null);
        IEnumerable<TableEntity> ReadStagedEntities(string chunk);
        void RemoveStagedEntities(string chunk);
    }

    public sealed class TableEntity
    {
        private const string IdProperty = "id";
        private const string TimestampProperty = "timestamp";

        public TableEntity(long id, DateTime timestamp, JObject content)
        {
            content[IdProperty] = id;
            content[TimestampProperty] = timestamp;
            Id = id;
            Timestamp = timestamp;
            Content = content;
        }

        public TableEntity(JObject content)
        {
            if (content[IdProperty] == null || content[TimestampProperty] == null)
                throw new ArgumentException("Missing id or timestamp in content.");

            Id = content[IdProperty].Value<long>();
            Timestamp = content[TimestampProperty].Value<DateTime>();
            Content = content;
        }

        public long Id { get; }
        public DateTime Timestamp { get; }
        public JObject Content { get; }

        public IDictionary<string, JToken> Properties => Content;
    }

    [Serializable]
    public class TableStoreException : Exception
    {
        public TableStoreException() { }
        public TableStoreException(string message) : base(message) { }
        public TableStoreException(string message, Exception inner) : base(message, inner) { }
        protected TableStoreException(
          System.Runtime.Serialization.SerializationInfo info,
          System.Runtime.Serialization.StreamingContext context) : base(info, context) { }
    }
}
