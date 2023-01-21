using System.Collections.Generic;

namespace Azure.EntityServices.Tables
{
    public interface IEntityOperationContext<T>
    {
        EntityOperation TableOperation { get; set; }
        string PartitionKey { get; set; } 
        string RowKey { get; set; }
        T Entity { get; set; }
        IDictionary<string, object> Metadata { get; set; }
    }
}