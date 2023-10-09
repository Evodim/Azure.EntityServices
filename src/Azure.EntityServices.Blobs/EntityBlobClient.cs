using Azure.EntityServices.Blobs.Extensions;
using Azure.EntityServices.Queries;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Azure.EntityServices.Blobs
{
    public class EntityBlobClient<T> : IEntityBlobClient<T>
        where T : class, new()
    {
        private readonly BlobService _blobService;
        private EntityBlobClientOptions _options;
        private EntityBlobClientConfig<T> _config;
        protected readonly IEnumerable<PropertyInfo> EntityProperties = typeof(T).GetProperties();

        public EntityBlobClient()
        {
        }

        public EntityBlobClient(BlobService blobService)
        {
            _blobService = blobService;
        }

        public EntityBlobClient<T> Configure(EntityBlobClientOptions options, EntityBlobClientConfig<T> config)
        {
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _config = config ?? throw new ArgumentNullException(nameof(config));
            _blobService.Configure(new BlobServiceOptions() { 
             ContainerName= options.ContainerName,
             MaxResultPerPage = options.MaxResultPerPage ?? 100
            });
            return this;
        }

        public async Task<BinaryData> GetContentAsync(string entityRef)
        {
            using var binaryStream = await _blobService.DownloadAsync(entityRef);

            return BinaryData.FromStream(binaryStream);
        }

        public Task<BinaryData> GetContentAsync(T entity)
        {
            return GetContentAsync(GetEntityReference(entity));
        }

        public Task<IDictionary<string, string>> GetPropsAsync(string entityRef)
        {
            return _blobService.GetBlobProperiesAsync(entityRef);
        }

        public Task<T> AddOrReplaceAsync(T entity, bool includeContent = false)
        {
            return AddOrReplaceAsync(ResolveEntityPath(entity), entity, includeContent);
        }

        private async Task<T> AddOrReplaceAsync(string entityPath, T entity, bool includeContent)
        {
            var blobRef = $"{entityPath}/{ResolveEntityName(entity)}";
            if (!includeContent)
            { 
                await _blobService.UpdatePropsAsync(blobRef, 
                    BuildAllTags(entity),
                    BuildAllProps(entity));
                return entity;
            }

            var value = _config.ContentProp?.GetValue(entity);
            var binaryContent = value switch
            {
                Stream v => BinaryData.FromStream(v),
                string v => BinaryData.FromString(v),
                byte[] v => BinaryData.FromBytes(v),
                BinaryData v => v,
                _ => BinaryData.FromObjectAsJson(value)
            };
            
            await _blobService.UploadAsync(
                blobRef,
                binaryContent.ToStream(),
                BuildAllTags(entity),
                BuildAllProps(entity));

            return entity;
        }

       

        public async IAsyncEnumerable<IReadOnlyList<IDictionary<string, string>>> ListPropsAsync(string entityPath)
        {
            await foreach (var page in _blobService.ListAsync(entityPath))
            {
                yield return page.Select(p => p).ToList();
            }
        }

        public async IAsyncEnumerable<IReadOnlyList<T>> ListAsync(string entityPath)
        {
            await foreach (var page in _blobService.ListAsync(entityPath))
            {
                yield return page.Select(p => BindEntityFromProperties(p)).ToList();
            }
        }

        public async IAsyncEnumerable<IReadOnlyList<T>> ListAsync(Action<IQuery<T>> query)
        {
            var queryExpr = new FilterExpression<T>(); 
            var baseQuery = queryExpr
                 .Where("@container").Equal(_options.ContainerName);

            if (query != null) baseQuery.And(query);

            var queryStr = new BlobTagQueryBuilder<T>(queryExpr).Build();

            await foreach (var page in _blobService.ListByTagsAsync(queryStr))
            {
                yield return page.Select(d => BindEntityFromProperties(d)).ToList();
            }
        }

        private IEnumerable<KeyValuePair<string, string>> BuildComputedProps(T entity)
        {
            return _config.ComputedProps.Select(p => new KeyValuePair<string, string>(p.Key, ValueToBlob(p.Value?.Invoke(entity))));
        }

        private IEnumerable<KeyValuePair<string, string>> BuildComputedIndexes(T entity)
        {
            return _config.ComputedIndexes
                .Select(p => new KeyValuePair<string, string>(p, ValueToBlob(_config.ComputedProps[p].Invoke(entity))));
        }

        private IEnumerable<KeyValuePair<string, string>> BuildEntityProps(T entity)
        {
            return EntityProperties
                 .Where(p => !_config.IgnoredProps.ContainsKey(p.Name))
                 .Select(p => new KeyValuePair<string, string>(p.Name, ValueToBlob(p.GetValue(entity))));
        }

        private IEnumerable<KeyValuePair<string, string>> BuildTags(T entity)
        {
            return _config.Tags
                .Select(p => new KeyValuePair<string, string>(p.Key, ValueToBlob(p.Value.GetValue(entity))));
        }

        private IDictionary<string, string> BuildAllProps(T entity)
        {
            return BuildComputedProps(entity)
                   .Union(BuildEntityProps(entity))
                   .AsDictionnary();
        }

        private IDictionary<string, string> BuildAllTags(T entity)
        {
            return BuildTags(entity)
                 .Union(BuildComputedIndexes(entity))
                 .AsDictionnary();
        }

        private T BindEntityFromProperties(IDictionary<string, string> blobProperties)
        {
            var entity = new T();

            foreach (var property in EntityProperties)
            {
                ValueToEntity(entity, property, blobProperties);
            }

            return entity;
        }

        public async Task<T> GetAsync(string entityRef)
        {
            var props = await _blobService.GetBlobProperiesAsync(entityRef);
            return BindEntityFromProperties(props);
        }
      
        public Task DropContainerAsync()
        {
            return _blobService.DeleteContainerAsync();
        }

        public Task DeleteAsync(string entityRef)
        {
            return _blobService.DeleteAsync(entityRef);
        }

        public string GetEntityReference(T entity)
        {
            return $"{ResolveEntityPath(entity)}/{ResolveEntityName(entity)}";
        }

        private string ResolveEntityPath(T entity)
        {
            if (_config.ComputedProps.TryGetValue("_EntityPath", out var resolver))
            {
                return $"{resolver.Invoke(entity)}";
            }
            throw new InvalidOperationException("EntityPath not configured");
        }

        private string ResolveEntityName(T entity)
        {
            if (_config.ComputedProps.TryGetValue("_EntityName", out var resolver))
            {
                return $"{resolver.Invoke(entity)}";
            }
            throw new InvalidOperationException("EntityName not configured");
        }

        private static string ValueToBlob(object value)
        {
            return value switch
            {
                byte[] v => Convert.ToBase64String(v),
                _ => Regex.Replace(value.ToInvariantString(), @"[^0-9a-zA-Z\+\-\.\/\:=_]+", "")
            };
        }

        private static void ValueToEntity(T entity, PropertyInfo property, IDictionary<string, string> blobProperties)
        {
            try
            {
                var propertyType = property.PropertyType;
                // Enforce public getter / setter
                if (property.GetSetMethod() == null ||
                    !property.GetSetMethod().IsPublic ||
                    property.GetGetMethod() == null ||
                    !property.GetGetMethod().IsPublic)
                {
                    return;
                }
                //Handle nullable types globally
                if (propertyType.IsGenericType && propertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
                {
                    propertyType = property.PropertyType.GetGenericArguments().First();
                }
                //Ignore other incompatible types

                // only proceed with properties that have a corresponding entry in the dictionary
                if (!blobProperties.TryGetValue(property.Name, out var strPropValue))
                {
                    return;
                }

                if (strPropValue == null)
                {
                    property.SetValue(entity, null, null);
                    return;
                }
                if (propertyType == typeof(byte[]))
                {
                    property.SetValue(entity, Convert.FromBase64String(strPropValue), null);
                    return;
                }
                if (propertyType == typeof(DateTime))
                {
                    if (DateTime.TryParse(strPropValue, out var value))
                    {
                        property.SetValue(entity, value, null);
                    }
                    return;
                }
                if (propertyType == typeof(int))
                {
                    if (int.TryParse(strPropValue, out var value))
                    {
                        property.SetValue(entity, value, null);
                    }
                    return;
                }
                if (propertyType == typeof(long))
                {
                    if (long.TryParse(strPropValue, out var value))
                    {
                        property.SetValue(entity, value, null);
                    }
                    return;
                }
                if (propertyType == typeof(DateTimeOffset))
                {
                    if (DateTimeOffset.TryParse(strPropValue, out var value))
                    {
                        property.SetValue(entity, value, null);
                    }
                    return;
                }
                if (propertyType == typeof(double))
                {
                    if (double.TryParse(strPropValue, NumberStyles.Any, CultureInfo.InvariantCulture, out var value))
                    {
                        property.SetValue(entity, value, null);
                    }
                    return;
                }
                if (propertyType == typeof(decimal))
                {
                    if (decimal.TryParse(strPropValue, NumberStyles.Any, CultureInfo.InvariantCulture, out var value))
                    {
                        property.SetValue(entity, value, null);
                    }
                    return;
                }
                if (propertyType == typeof(float))
                {
                    if (float.TryParse(strPropValue, NumberStyles.Any, CultureInfo.InvariantCulture, out var value))
                    {
                        property.SetValue(entity, value, null);
                    }
                    return;
                }
                if (propertyType.IsEnum)
                {
                    if (Enum.TryParse(propertyType, strPropValue, out var parsedEnum))
                    {
                        property.SetValue(entity, parsedEnum, null);
                    }
                    return;
                }

                if (propertyType != typeof(BinaryData) &&
                    propertyType != typeof(string) &&
                    propertyType.IsClass &&
                    !propertyType.IsValueType)
                {
                    //otherwise  it should be a serialized object
                    property.SetValue(entity, JsonSerializer.Deserialize(strPropValue, propertyType), null);
                    return;
                }
                property.SetValue(entity, strPropValue, null);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Unable to bind entity property {property.Name} with type  {property.PropertyType.Name}", ex);
            }
        } 
    }
}