using System.Collections.Generic;

namespace Azure.EntityServices.Tables
{
    internal class EntityOperationContext<T> : IEntityOperationContext<T>
    {
        public EntityOperation TableOperation { get; set; }
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public T Entity { get; set; }
        public IDictionary<string, object> Metadata { get; set; } = new Dictionary<string, object>();
    }
}