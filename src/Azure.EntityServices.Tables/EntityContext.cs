using Azure.EntityServices.Tables.Core;

namespace Azure.EntityServices.Tables
{
    public record struct EntityContext<T>(string PartitionKey, string RowKey, IEntityDataReader<T> EntityDataReader, EntityOperation EntityOperation);
}