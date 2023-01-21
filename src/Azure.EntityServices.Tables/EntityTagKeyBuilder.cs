using Azure.EntityServices.Tables.Core;
using System;
using System.Reflection;

namespace Azure.EntityServices.Tables
{
    public sealed class EntityTagBuilder<T>
    {
        private readonly Func<T, string> _primaryKeyResolver;

        public EntityTagBuilder(Func<T,string> primaryKeyResolver)
        {
            _primaryKeyResolver = primaryKeyResolver;
        }
        public string IndexedTagSuffix => "_indexed_tag_";

        public string CreateTagName(string tagName) => $"{tagName}{IndexedTagSuffix}";
        public string CreateTagRowKey(PropertyInfo property, T entity) => $"{TableQueryHelper.ToTagRowKeyPrefix(property.Name, property.GetValue(entity))}{_primaryKeyResolver(entity)}";
        public string CreateTagRowKey(string key, object value, T entity) => $"{TableQueryHelper.ToTagRowKeyPrefix(key, value)}{_primaryKeyResolver(entity)}";

    }
}