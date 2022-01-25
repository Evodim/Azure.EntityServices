using Azure.EntityServices.Tables.Extensions;
using System;
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

        public static EntityTableClientConfig<T> SetPrimaryKeyProp<T, P>(this EntityTableClientConfig<T> config, Expression<Func<T, P>> selector)
        {
            var property = selector.GetPropertyInfo();
            config.PrimaryKeyProp = property;
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

        public static EntityTableClientConfig<T> AddObserver<T>(this EntityTableClientConfig<T> config, string observerName, IEntityObserver<T> entityObserver)
        {
            config.Observers.TryAdd(observerName, entityObserver);
            return config;
        }

        public static EntityTableClientConfig<T> RemoveObserver<T>(this EntityTableClientConfig<T> config, string observerName)
        {
            config.Observers.TryRemove(observerName, out var _);
            return config;
        }
    }
}