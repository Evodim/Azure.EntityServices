namespace Azure.EntityServices.Tables.Core
{
    public class TableBatchClientOptions
    {
        public string TableName { get; set; } 
        public int MaxItemInBatch { get; set; } = 1000;
        public int MaxItemInTransaction { get; set; } = 100;
        public int MaxParallelTasks { get; set; } = 1;
    }
}