using Azure.Data.Tables;
using System.Collections.Generic;

namespace Azure.EntityServices.Tables.Core
{
    public interface IEntityBinder<T> where T : class, new()
    {
        string PartitionKey { get; }
        string RowKey { get; }
        T Entity { get; set; }
        IDictionary<string, object> Properties { get; }
        IDictionary<string, object> Metadata { get; } 
        TableEntity Bind(); 
        T UnBind();
    }
}