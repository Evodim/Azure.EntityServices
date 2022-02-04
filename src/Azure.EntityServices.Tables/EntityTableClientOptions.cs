namespace Azure.EntityServices.Tables
{
    public class EntityTableClientOptions
    {
        public EntityTableClientOptions()
        {
        }

        public EntityTableClientOptions(string connectionString,
            string tableName,
            int maxParallelTransactions = 4,
            int maxItemToGroup = 1000,
            int maxItemInTransaction = 100,
            bool createTableIfNotExists = false)
        {
            ConnectionString = connectionString;
            TableName = tableName;
            MaxParallelTransactions = maxParallelTransactions;
            MaxItemInTransaction = maxItemInTransaction;
            MaxItemInBatch = maxItemToGroup;
            CreateTableIfNotExists = createTableIfNotExists;
        }

        public bool CreateTableIfNotExists { get; set; }
        public string ConnectionString { get; set; }
        public string TableName { get; set; }
        public int MaxParallelTransactions { get; set; }
        public int MaxItemInTransaction { get; set; }
        public int MaxItemInBatch { get; set; }
    }
}