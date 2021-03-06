using Azure.Data.Tables;
using Azure.EntityServices.Tables.Extensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Azure.EntityServices.Tables.Core
{
    /// <summary>
    /// Entity binder used to bind pure entity and his metadata to Azure tableEntity
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public sealed class TableEntityBinder<T> : IEntityBinder<T> where T : class, new()
    {
        public string PartitionKey => _tableEntity.PartitionKey;
        public string RowKey => _tableEntity.RowKey;

        private readonly TableEntity _tableEntity;
        public T Entity { get; set; }

        private readonly IEnumerable<string> _propsToIgnore = new List<string>(); 
        public IDictionary<string, object> Properties { get; } = new Dictionary<string, object>();
        public IDictionary<string, object> Metadata { get; } = new Dictionary<string, object>();

        public static readonly IEnumerable<PropertyInfo> EntityProperties = typeof(T).GetProperties();

        public TableEntityBinder(T entity)
        {
            Entity = entity;
            _tableEntity = new TableEntity();
        }

        public TableEntityBinder(T entity, IEnumerable<string> propsToIgnore)
        {
            _propsToIgnore = propsToIgnore;
            Entity = entity;
            _tableEntity = new TableEntity();
        }

        public TableEntityBinder(TableEntity tableEntity)
        {
            _tableEntity = tableEntity;
        }

        public TableEntityBinder(TableEntity tableEntity, IEnumerable<string> propsToIgnore)
        {
            _tableEntity = tableEntity;
            _propsToIgnore = propsToIgnore;
        }

        public TableEntityBinder(string partitionKey, string rowKey)
        {
            _tableEntity = new TableEntity(partitionKey, rowKey);
        }

        public TableEntityBinder(T entity, string partitionKey, string rowKey)
        {
            Entity = entity;
            _tableEntity = new TableEntity(partitionKey, rowKey);
        }

        public TableEntityBinder(T entity, string partitionKey, string rowKey, IEnumerable<string> propsToIgnore)
        {
            Entity = entity;
            _tableEntity = new TableEntity(partitionKey, rowKey);
            _propsToIgnore = propsToIgnore;
        }

        public TableEntity Bind()
        {
            
            foreach (var metadata in Metadata)
            {
                _tableEntity.AddOrUpdate(metadata.Key, EntityValueAdapter.WriteValue(metadata.Value));
            }
            foreach (var property in EntityProperties.Where(p => !_propsToIgnore.Contains(p.Name)))
            {
                _tableEntity.AddOrUpdate(property.Name, EntityValueAdapter.WriteValue(property.GetValue(Entity), property));
            }
            return _tableEntity;
        }

        public T UnBind()
        {
            Entity = new T();
            Metadata.Clear();
            foreach (var tableProp in _tableEntity)
            {
                //ignore entity properties
                if (EntityProperties.Any(p => p.Name == tableProp.Key)) continue;
                //ignore system properties
                if (tableProp.Key == TableConstants.RowKey ||
                    tableProp.Key == TableConstants.PartitionKey ||
                    tableProp.Key == TableConstants.Timestamp) continue;

                Metadata.Add(tableProp.Key, tableProp.Value);
            }
            foreach (var property in EntityProperties.Where(p => !_propsToIgnore.Contains(p.Name)))
            {
                if (_tableEntity.TryGetValue(property.Name, out var tablePropValue))
                {
                    EntityValueAdapter.ReadValue(Entity, property, tablePropValue);
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
    }
}