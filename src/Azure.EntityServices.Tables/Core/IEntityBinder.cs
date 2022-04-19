using Azure.Data.Tables;
using System;
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
        void BindDynamicProps(IDictionary<string, Func<T, object>> props, bool toDelete = false); 
        T UnBind();
    }
}