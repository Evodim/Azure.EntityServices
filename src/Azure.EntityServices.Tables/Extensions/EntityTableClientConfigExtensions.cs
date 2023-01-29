using Azure.EntityServices.Tables.Extensions;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Azure.EntityServices.Tables
{
    public static partial class EntityTableClientConfigExtensions
    {
        public static EntityTableClientConfig<T> SetPartitionKey<T>(this EntityTableClientConfig<T> config, Func<T, string> partitionKeyResolver)
        {
            config.PartitionKeyResolver = partitionKeyResolver;
            return config;
        }

        public static EntityTableClientConfig<T> SetRowKeyProp<T, P>(this EntityTableClientConfig<T> config, Expression<Func<T, P>> selector)
        {
            var property = selector.GetPropertyInfo();
            config.RowKeyProp = property;
            config.RowKeyResolver = null;
            return config;
        }

        public static EntityTableClientConfig<T> SetRowKey<T>(this EntityTableClientConfig<T> config, Func<T, string> rowKeyResolver)
        {
            config.RowKeyProp = null;
            config.RowKeyResolver = rowKeyResolver;
            return config;
        }

        public static EntityTableClientConfig<T> AddTag<T, P>(this EntityTableClientConfig<T> config, Expression<Func<T, P>> selector)
        {
            var property = selector.GetPropertyInfo();
            config.Tags.Add(property.Name, property);
            return config;
        }

        public static EntityTableClientConfig<T> AddTag<T>(this EntityTableClientConfig<T> config, string propName)
        {
            config.ComputedTags.Add(propName);
            return config;
        }

        public static EntityTableClientConfig<T> AddComputedProp<T>(this EntityTableClientConfig<T> config, string propName, Func<T, object> propValue)
        {
            config.DynamicProps.Add(propName, propValue);
            return config;
        }

        public static EntityTableClientConfig<T> AddObserver<T>(this EntityTableClientConfig<T> config, string observerName, Func<IEntityObserver<T>> entityObserverFactory)
        {
            config.Observers.TryAdd(observerName, entityObserverFactory);
            return config;
        }

        public static EntityTableClientConfig<T> RemoveObserver<T>(this EntityTableClientConfig<T> config, string observerName)
        {
            config.Observers.TryRemove(observerName, out var _);
            return config;
        }

        public static EntityTableClientConfig<T> IgnoreProp<T, P>(this EntityTableClientConfig<T> config, Expression<Func<T, P>> selector)
        {
            var property = selector.GetPropertyInfo();

            config?.IgnoredProps.Add(property.Name);
            return config;
        }
    }
}