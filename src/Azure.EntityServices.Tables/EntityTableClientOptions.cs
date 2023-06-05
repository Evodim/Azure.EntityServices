using System.Text.Json;

namespace Azure.EntityServices.Tables
{
    public class EntityTableClientOptions
    {
        public EntityTableClientOptions()
        {
        }

        public EntityTableClientOptions(
            string tableName,
            int maxParallelTransactions = -1,
            int maxItemToGroup = 1000,
            int maxItemInTransaction = 100,
            bool createTableIfNotExists = false,
            bool handleTagMutation = false,
            JsonSerializerOptions serializerOptions = default
            )
        {
            TableName = tableName;
            MaxParallelTransactions = maxParallelTransactions;
            MaxOperationPerTransaction = maxItemInTransaction;
            MaxItemToGroup = maxItemToGroup;
            CreateTableIfNotExists = createTableIfNotExists;
            HandleTagMutation = handleTagMutation;
            SerializerOptions = serializerOptions ?? new JsonSerializerOptions();
        }

        public bool CreateTableIfNotExists { get; set; }
        public string TableName { get; set; }
        public int MaxParallelTransactions { get; set; } = -1;
        public int MaxOperationPerTransaction { get; set; } = 100;
        public int MaxItemToGroup { get; set; } = 1000;
        public bool HandleTagMutation { get; set; }
        public JsonSerializerOptions SerializerOptions { get; set; } = new();
    }
}