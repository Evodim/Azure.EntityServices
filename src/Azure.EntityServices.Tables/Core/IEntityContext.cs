using Azure.EntityServices.Tables.Core;

namespace Azure.EntityServices.Tables
{
    public interface IEntityContext<T> 
    {
        string PartitionKey { get; }
        string RowKey { get; }
        IEntityDataReader<T> EntityDataReader { get; }
        EntityOperationType EntityOperation { get; }
    }
}