using System.Collections.Generic;

namespace Azure.EntityServices.Table
{
    public interface IEntityOperationContext<T>
    {
        EntityOperation TableOperation { get; set; }
        string Partition { get; set; }
        T Entity { get; set; }
        IDictionary<string, object> Metadatas { get; set; }
    }
}