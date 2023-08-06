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
    public sealed class TableEntityAdapter<T> : IEntityAdapter<T, TableEntity> where T : class, new()
    {
        public string PartitionKey => TableEntity.PartitionKey;
        public string RowKey => TableEntity.RowKey;

        public TableEntity TableEntity { get; }

        private readonly JsonSerializerOptions _serializerOptions;
        private readonly IDictionary<string, Func<T, object>> _computedProps;
        private readonly Dictionary<string, PropertyInfo> _tags;
        private readonly IList<string> _computedTags;

        public T Entity { get; private set; }
        public IDictionary<string, object> Properties { get; } = new Dictionary<string, object>();
        public IDictionary<string, object> Metadata { get; } = new Dictionary<string, object>();

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
            IDictionary<string, Func<T, object>> computedProps,
            Dictionary<string, PropertyInfo> tags,
            IList<string> computedTags,
            IEnumerable<string> propsToIgnore = null,
            JsonSerializerOptions serializerOptions = null)
        {
            _propsToIgnore = propsToIgnore ?? Enumerable.Empty<string>();
            _filteredEntityProperties = FilterEntityProperties();
            _entityKeyBuilder = entityKeyBuilder;
            _serializerOptions = serializerOptions;
            _computedProps = computedProps;
            _tags = tags;
            _computedTags = computedTags;


        }
        public TableEntityAdapter(
            TableEntity tableEntity,
            EntityKeyBuilder<T> entityKeyBuilder,
            IEnumerable<string> propsToIgnore = null,
            JsonSerializerOptions serializerOptions = null)
        {
            _ = entityKeyBuilder ?? throw new ArgumentNullException(nameof(entityKeyBuilder));

            TableEntity = tableEntity;
            _propsToIgnore = propsToIgnore ?? Enumerable.Empty<string>();
            _filteredEntityProperties = FilterEntityProperties();
            _entityKeyBuilder = entityKeyBuilder;
            _serializerOptions = serializerOptions;
        }

        public TableEntityAdapter(
            T entity,
            EntityKeyBuilder<T> entityKeyBuilder,
            IEnumerable<string> propsToIgnore = null,
            JsonSerializerOptions serializerOptions = null)
        {
            _ = entityKeyBuilder ?? throw new ArgumentNullException(nameof(entityKeyBuilder));

            Entity = entity;
            var partitionKey = entityKeyBuilder.ResolvePartitionKey(entity);
            var rowKey = entityKeyBuilder.ResolvePrimaryKey(entity);

            TableEntity = new TableEntity(partitionKey, rowKey);
            _propsToIgnore = propsToIgnore ?? Enumerable.Empty<string>();
            _filteredEntityProperties = FilterEntityProperties();
            _entityKeyBuilder = entityKeyBuilder;
            _serializerOptions = serializerOptions;
        }
        public TableEntity WriteToEntityModel(T entity)
        {
            var metadata = new Dictionary<string,object>();
            BindDynamicProps(metadata, entity);
            BindTags(metadata, entity);

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
            foreach (var property in Properties)
            {
                tableEntity.AddOrUpdate(property.Key, EntityValueAdapter.WriteValue(property.Value, _serializerOptions));
            }
            foreach (var dataField in metadata)
            {
                tableEntity.AddOrUpdate(dataField.Key, EntityValueAdapter.WriteValue(dataField.Value, _serializerOptions));
            }
            return tableEntity;

        }

        public T ReadFromEntityModel(TableEntity tableEntity)
        {
            var entity = new T();
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
        
        public TableEntity WriteToEntityModel()
        {
            if (Entity is TableEntity tbe)
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
                    TableEntity.AddOrUpdate(property.Key, property.Value);
                }
            }
            else
            {
                foreach (var property in _filteredEntityProperties)
                {
                    TableEntity.AddOrUpdate(property.Name, EntityValueAdapter.WriteValue(property.GetValue(Entity), _serializerOptions, property));
                }
            }
            foreach (var property in Properties)
            {
                TableEntity.AddOrUpdate(property.Key, EntityValueAdapter.WriteValue(property.Value, _serializerOptions));
            }
            foreach (var metadata in Metadata)
            {
                TableEntity.AddOrUpdate(metadata.Key, EntityValueAdapter.WriteValue(metadata.Value, _serializerOptions));
            }
            return TableEntity;
        }

        public T ReadFromEntityModel()
        {
            Entity = new T();
            Metadata.Clear();
            foreach (var tableProp in TableEntity)
            {
                //ignore entity properties
                if (EntityProperties.Any(p => p.Name == tableProp.Key)) continue;
                //ignore system properties
                if (tableProp.Key == TableConstants.RowKey ||
                    tableProp.Key == TableConstants.PartitionKey ||
                    tableProp.Key == TableConstants.Timestamp) continue;

                Metadata.Add(tableProp.Key, tableProp.Value);
            }
            if (Entity is TableEntity tbe)
            {
                foreach (var property in TableEntity.Keys)
                {
                    tbe.AddOrUpdate(property, TableEntity[property]);
                }
            }
            else
                foreach (var property in _filteredEntityProperties)
                {
                    if (TableEntity.TryGetValue(property.Name, out var tablePropValue))
                    {
                        EntityValueAdapter.ReadValue(Entity, property, _serializerOptions, tablePropValue);
                    }
                }

            return Entity;
        }

        public void BindDynamicProps(IDictionary<string, Func<T, object>> props, bool toDelete = false)
        {
            foreach (var prop in props)
            {
                if (toDelete && Metadata.ContainsKey(prop.Key))
                {
                    Metadata.Remove(prop.Key);
                    continue;
                }
                Metadata.AddOrUpdate(prop.Key, prop.Value.Invoke(Entity));
            }
        }

        public void BindTags(Dictionary<string, PropertyInfo> tags, IList<string> computedTags)
        {
            foreach (var propInfo in tags)
            {
                Metadata.AddOrUpdate(_entityKeyBuilder.CreateTagName(propInfo.Key), _entityKeyBuilder.CreateTagRowKey(propInfo.Value, Entity));
            }
            foreach (var tagPrefix in computedTags)
            {
                Metadata.AddOrUpdate(_entityKeyBuilder.CreateTagName(tagPrefix), _entityKeyBuilder.CreateTagRowKey(tagPrefix, Metadata[$"{tagPrefix}"], Entity));
            }
        }

        public Dictionary<string, object> BindDynamicProps(Dictionary<string, object> metadata, T entity, bool toDelete= false)
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
        public Dictionary<string, object> BindTags(Dictionary<string, object> metadata, T entity)
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