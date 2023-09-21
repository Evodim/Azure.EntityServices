using System;
using System.Reflection;

namespace Azure.EntityServices.Tables.Core
{

    public sealed class EntityKeyBuilder<T>
    {
        private readonly Func<T, object> _primaryKeyResolver;
        private readonly Func<T, string> _partitionKeyResolver;

        public EntityKeyBuilder(Func<T, string> partitionKeyResolver, Func<T, object> primaryKeyResolver)
        {
            _primaryKeyResolver = primaryKeyResolver;
            _partitionKeyResolver = partitionKeyResolver;
        }

        public string IndexedTagSuffix => "_indexed_tag_";

        public string CreateTagName(string tagName) => $"{tagName}{IndexedTagSuffix}";

        public string CreateTagRowKey(PropertyInfo property, T entity) => $"{TableQueryHelper.ToTagRowKeyPrefix(property.Name, property.GetValue(entity))}{ResolvePrimaryKey(entity)}";

        public string CreateTagRowKey(string key, object value, T entity) => $"{TableQueryHelper.ToTagRowKeyPrefix(key, value)}{ResolvePrimaryKey(entity)}";

        public string ResolvePartitionKey(T entity) => TableQueryHelper.ToPartitionKey(_partitionKeyResolver(entity) ?? throw new EntityTableClientException("Given partitionKey is null") { });

        public string ResolvePrimaryKey(T entity)
        {
            return TableQueryHelper.ToPrimaryRowKey(_primaryKeyResolver.Invoke(entity) ?? throw new EntityTableClientException("Given primaryKey is null") { });
        }

        public string ResolvePrimaryKey(object value)
        {
            return TableQueryHelper.ToPrimaryRowKey(value ?? throw new EntityTableClientException("Given primaryKey is null") { });
        }
    }
}