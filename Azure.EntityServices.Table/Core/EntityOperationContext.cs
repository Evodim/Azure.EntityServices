using System.Collections.Generic;

namespace Azure.EntityServices.Table.Core
{
    internal class EntityOperationContext<T> : IEntityOperationContext<T>
    {
        public EntityOperation TableOperation { get; set; }
        public string Partition { get; set; }
        public T Entity { get; set; }
        public IDictionary<string, object> Metadatas { get; set; } = new Dictionary<string, object>();
    }
}