namespace Azure.EntityServices.Tables.Core.Abstractions
{
    public interface IEntityContext<T>
    {
        string PartitionKey { get; }
        string RowKey { get; }
        IEntityDataReader<T> EntityDataReader { get; }
        EntityOperationType EntityOperation { get; }
    }
}