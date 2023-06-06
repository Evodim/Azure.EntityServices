using System;
using System.Collections.Generic;
using System.Reflection;

namespace Azure.EntityServices.Tables.Core
{
    public interface IEntityAdapter<T>
    {
        string PartitionKey { get; }
        string RowKey { get; }
        IDictionary<string, object> Properties { get; }
        IDictionary<string, object> Metadata { get; }

        void BindDynamicProps(IDictionary<string, Func<T, object>> props, bool toDelete = false);

        void BindTags(Dictionary<string, PropertyInfo> tags, IList<string> computedTags);

        T ReadFromEntityModel();
    }

    public interface IEntityAdapter<T, TEntityModel> : IEntityAdapter<T>
    {
        TEntityModel WriteToEntityModel();
    }
}