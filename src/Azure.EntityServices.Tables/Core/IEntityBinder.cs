using Azure.Data.Tables;
using System;
using System.Collections.Generic;
using System.Reflection;

namespace Azure.EntityServices.Tables.Core
{
    public interface IEntityBinder<T> 
    {
        string PartitionKey { get; }
        string RowKey { get; }
        TableEntity TableEntity { get; } 
        IDictionary<string, object> Properties { get; }
        IDictionary<string, object> Metadata { get; }  
        void BindDynamicProps(IDictionary<string, Func<T, object>> props, bool toDelete = false);
        void BindTags(Dictionary<string, PropertyInfo> tags, IList<string> computedTags); 
        T UnBind();
    }

    public interface IEntityBinder<T,TEntityModel>: IEntityBinder<T>
    {
        TEntityModel Bind();
        
    }
}