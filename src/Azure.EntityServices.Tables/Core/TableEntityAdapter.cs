using Azure.Data.Tables;
using Azure.EntityServices.Tables.Extensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.Json;

namespace Azure.EntityServices.Tables.Core
{
    /// <summary>
    /// Entity adapter used to map pure entity and his metadata to Azure tableEntity
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public sealed class TableEntityAdapter<T> : IEntityAdapter<T, TableEntity>
    where T : class, new()
    {
        private readonly JsonSerializerOptions _serializerOptions;
        private readonly IReadOnlyDictionary<string, Func<T, object>> _computedProps;
        private readonly IReadOnlyDictionary<string, PropertyInfo> _tags;
        private readonly IEnumerable<string> _computedTags;

        public static readonly IEnumerable<PropertyInfo> EntityProperties = typeof(T).GetProperties(
            BindingFlags.Public |
            BindingFlags.Instance |
            BindingFlags.SetProperty);

        private readonly IEnumerable<PropertyInfo> _filteredEntityProperties = EntityProperties.ToList();
        private readonly EntityKeyBuilder<T> _entityKeyBuilder;
        private readonly IEnumerable<string> _propsToIgnore = Enumerable.Empty<string>();

        private List<PropertyInfo> FilterEntityProperties() => EntityProperties.Where(p => !_propsToIgnore.Contains(p.Name)).ToList();

        public TableEntityAdapter(
            EntityKeyBuilder<T> entityKeyBuilder,
            IReadOnlyDictionary<string, Func<T, object>> computedProps = null,
            IReadOnlyDictionary<string, PropertyInfo> tags = null,
            IReadOnlyCollection<string> computedTags = null,
            IReadOnlyCollection<string> propsToIgnore = null,
            JsonSerializerOptions serializerOptions = null)
        {
            _filteredEntityProperties = FilterEntityProperties();
            _entityKeyBuilder = entityKeyBuilder;
            _propsToIgnore = propsToIgnore ?? Enumerable.Empty<string>();
            _computedProps = computedProps ?? new Dictionary<string, Func<T, object>>();
            _tags = tags ?? new Dictionary<string, PropertyInfo>();
            _computedTags = computedTags ?? Enumerable.Empty<string>();
            _serializerOptions = serializerOptions;
        }

        public TableEntity ToEntityModel(T entity)
        {
            var metadata = new Dictionary<string, object>();
            GenerateDynamicProps(metadata, entity);
            GenerateTagProps(metadata, entity);

            if (entity == null)
            {
                throw new ArgumentNullException(nameof(entity));
            }
            var tableEntity = new TableEntity(_entityKeyBuilder.ResolvePartitionKey(entity), _entityKeyBuilder.ResolvePrimaryKey(entity));
            if (entity is TableEntity tbe)
            {
                foreach (var property in tbe.Where(e => !_propsToIgnore.Contains(e.Key)))
                {
                    if (property.Key == "PartitionKey" ||
                        property.Key == "RowKey" ||
                        property.Key == "Etag" ||
                        property.Key == "TimeStamp")
                    {
                        continue;
                    }
                    tableEntity.AddOrUpdate(property.Key, property.Value);
                }
            }
            else
            {
                foreach (var property in _filteredEntityProperties)
                {
                    tableEntity.AddOrUpdate(property.Name, EntityValueAdapter.WriteValue(property.GetValue(entity), _serializerOptions, property));
                }
            }

            foreach (var dataField in metadata)
            {
                tableEntity.AddOrUpdate(dataField.Key, EntityValueAdapter.WriteValue(dataField.Value, _serializerOptions));
            }
            return tableEntity;
        }

        public T FromEntityModel(TableEntity tableEntity)
        {
            var entity = new T();

            if (entity is TableEntity tbe)
            {
                foreach (var property in tableEntity.Keys)
                {
                    tbe.AddOrUpdate(property, tableEntity[property]);
                }
            }
            else
                foreach (var property in _filteredEntityProperties)
                {
                    if (tableEntity.TryGetValue(property.Name, out var tablePropValue))
                    {
                        EntityValueAdapter.ReadValue(entity, property, _serializerOptions, tablePropValue);
                    }
                }

            return entity;
        }

        public IDictionary<string, object> GetProperties(TableEntity tableEntity)
        {
            var metadata = new Dictionary<string, object>();

            foreach (var tableProp in tableEntity)
            {
                //ignore entity properties
                if (EntityProperties.Any(p => p.Name == tableProp.Key)) continue;
                //ignore system properties
                if (tableProp.Key == TableConstants.RowKey ||
                    tableProp.Key == TableConstants.PartitionKey ||
                    tableProp.Key == TableConstants.Timestamp) continue;

                metadata.Add(tableProp.Key, tableProp.Value);
            }
            return metadata;
        }

        private Dictionary<string, object> GenerateDynamicProps(Dictionary<string, object> metadata, T entity, bool toDelete = false)
        {
            foreach (var prop in _computedProps)
            {
                if (toDelete && metadata.ContainsKey(prop.Key))
                {
                    metadata.Remove(prop.Key);
                    continue;
                }
                metadata.AddOrUpdate(prop.Key, prop.Value.Invoke(entity));
            }
            return metadata;
        }

        private Dictionary<string, object> GenerateTagProps(Dictionary<string, object> metadata, T entity)
        {
            foreach (var propInfo in _tags)
            {
                metadata.AddOrUpdate(_entityKeyBuilder.CreateTagName(propInfo.Key), _entityKeyBuilder.CreateTagRowKey(propInfo.Value, entity));
            }
            foreach (var tagPrefix in _computedTags)
            {
                metadata.AddOrUpdate(_entityKeyBuilder.CreateTagName(tagPrefix), _entityKeyBuilder.CreateTagRowKey(tagPrefix, metadata[$"{tagPrefix}"], entity));
            }
            return metadata;
        }
    }
}