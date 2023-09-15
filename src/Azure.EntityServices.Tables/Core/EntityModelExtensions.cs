using Azure.Data.Tables;
using Azure.EntityServices.Tables.Extensions;

namespace Azure.EntityServices.Tables.Core
{
    public static class EntityModelExtensions
    {
        public static TableEntity ToTableEntityModel<T>(this EntityOperation entityOperation)
        {
            var tableEntity = new TableEntity(entityOperation.PartitionKey, entityOperation.RowKey);
            foreach (var property in entityOperation.NativeProperties)
            {
                if (property.Key == "PartitionKey" ||
                    property.Key == "RowKey" ||
                    property.Key == "Etag" ||
                    property.Key == "TimeStamp")
                {
                    continue;
                }
                tableEntity.AddOrUpdate(property.Key, property.Value);
            }
            return tableEntity;
        } 
    }
}