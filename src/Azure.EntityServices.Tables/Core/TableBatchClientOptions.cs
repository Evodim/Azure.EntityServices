namespace Azure.EntityServices.Tables.Core
{
    public class TableBatchClientOptions
    {
        public string TableName { get; set; }
        public string ConnectionString { get; set; }
        public int MaxItemPerTransaction { get; set; } = 100;
        public int MaxParallelTasks { get; set; }=1;
    }
}