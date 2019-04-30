using System;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;

namespace GiantBombDataTool
{
    public interface IReadOnlyTableStore
    {
        IEnumerable<TableEntity> GetEntitiesByTimestamp(
            string table,
            TableConfig config,
            DateTime? lastTimestamp,
            long? lastId);
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
            Content = content;
        }

        public TableEntity(JObject content)
        {
            if (content[IdProperty] == null || content[TimestampProperty] == null)
                throw new ArgumentException("Missing id or timestamp in content.");

            Content = content;
        }

        // TODO: Consider shredding content into Dictionary<string, object> Properties
        // and using System.Text.Json.JsonElement for non-primitive property values
        // Would need to implement extension method for writing JsonElement to a Utf8JsonWriter

        public long Id => Content[IdProperty].Value<long>();
        public DateTime Timestamp => Content[TimestampProperty].Value<DateTime>();
        public JObject Content { get; }
    }
}
