using Azure.EntityServices.Queries;
using Azure.EntityServices.Blobs.Extensions;
using System;
using System.Linq.Expressions;

namespace Azure.EntityServices.Blobs
{
    public static partial class EntityBlobClientConfigExtensions
    {
        
        public static EntityBlobClientConfig<T> SetContentProp<T>(this EntityBlobClientConfig<T> config, Expression<Func<T, object>> selector) 
        {
            var property = selector.GetPropertyInfo();
            config.ContentProp = property;
            config.IgnoreProp(selector);
            return config;
        }

        public static EntityBlobClientConfig<T> SetEntityName<T>(this EntityBlobClientConfig<T> config, Func<T, string> blobNameResolver)
        {
            return config?.AddComputedProp("_EntityName", blobNameResolver);

        }
        public static EntityBlobClientConfig<T> SetEntityPath<T>(this EntityBlobClientConfig<T> config, Func<T, string> blobNameResolver)
        {
            return config?.AddComputedProp("_EntityPath", blobNameResolver);

        }     

        public static EntityBlobClientConfig<T> IgnoreProp<T, P>(this EntityBlobClientConfig<T> config, Expression<Func<T, P>> selector)
        {
            var property = selector.GetPropertyInfo();

            config?.IgnoredProps.Add(property.Name, property);
            return config;
        }

        public static EntityBlobClientConfig<T> AddTag<T, P>(this EntityBlobClientConfig<T> config, Expression<Func<T, P>> selector)
        {
            var property = selector.GetPropertyInfo();

            config?.Indexes.Add(property.Name, property);
            return config;
        }

        public static EntityBlobClientConfig<T> AddTag<T>(this EntityBlobClientConfig<T> config, string propName)
        {
            config?.ComputedIndexes.Add(propName);
            return config;
        }

        public static EntityBlobClientConfig<T> AddComputedProp<T>(this EntityBlobClientConfig<T> config, string propName, Func<T, object> propValue)
        {
            config?.ComputedProps.Add(propName, propValue);
            return config;
        }
    }
}