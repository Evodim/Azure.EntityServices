using Azure.EntityServices.Tables.Core;

namespace Azure.EntityServices.Tables
{
    public class EntityContext<T>  : IEntityContext<T>
    where T : class, new()
    {
        public IEntityDataReader<T> EntityDataReader { get; }

        public EntityOperation EntityOperation { get; }

        public string PartitionKey { get; }

        public string RowKey { get; }
         
        public EntityContext(string partionKey, string rowKey, IEntityDataReader<T> entityDataReader, EntityOperation entityOperation)
        {
            PartitionKey = partionKey;
            RowKey = rowKey;
            EntityDataReader = entityDataReader;
            EntityOperation = entityOperation;
        }
     
    }
}