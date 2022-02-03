namespace Azure.EntityServices.Tables
{
    public class EntityTableClientOptions
    {
        public EntityTableClientOptions()
        {
        }

        public EntityTableClientOptions(string connectionString,
            string tableName,
            int maxParallelTasks = 10,
            int maxItemsPerBatch = 2000,
            bool createTableIfNotExists = false)
        {
            ConnectionString = connectionString;
            TableName = tableName;
            MaxParallelTasks = maxParallelTasks;
            MaxItemsPerInsertion = maxItemsPerBatch;
            CreateTableIfNotExists = createTableIfNotExists;
        }

        public bool CreateTableIfNotExists { get; set; }
        public string ConnectionString { get; set; }
        public string TableName { get; set; }
        public int MaxParallelTasks { get; set; }
        public int MaxItemsPerInsertion { get; set; }
    }
}