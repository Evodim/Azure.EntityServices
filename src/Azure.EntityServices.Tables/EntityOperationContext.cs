using Azure.EntityServices.Tables.Core.Abstractions;
using System.Collections.Generic;

namespace Azure.EntityServices.Tables
{
    public record struct EntityModel(string PartitionKey, string RowKey, IDictionary<string, object> NativeProperties);
    public record struct EntityOperation(string PartitionKey,string RowKey,EntityOperationType EntityOperationType, IDictionary<string,object> NativeProperties);
    public record struct EntityOperationContext<T>(string PartitionKey, string RowKey, IEntityDataReader<T> EntityDataReader, EntityOperationType EntityOperation);
}