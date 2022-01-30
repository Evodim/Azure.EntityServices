using Azure.Data.Tables;
using Azure.EntityServices.Queries;
using Azure.EntityServices.Tables.Core;
using Azure.EntityServices.Tables.Extensions;
using Polly;
using Polly.Retry;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Azure.EntityServices.Tables
{
    /// <summary>
    /// Client to manage pure entities in azure Tables
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class EntityTableClient<T> : IEntityTableClient<T>
    where T : class, new()
    {
        protected const string Deleted_suffix = "_deleted_";
        protected const string Tag_suffix = "_tag_";
        protected readonly Func<string, string> TagName = (tagName) => $"{tagName}{Tag_suffix}";

        private readonly EntityTableClientConfig<T> _config;
        private readonly EntityTableClientOptions _options;
        private readonly TableClient _client;
        private readonly TableServiceClient _tableService;
        private readonly AsyncRetryPolicy _retryPolicy;

        public EntityTableClient(EntityTableClientOptions options, Action<EntityTableClientConfig<T>> configurator = null)
        {
            _options = options;
            _client = new TableClient(options.ConnectionString, options.TableName);
            _tableService = new TableServiceClient(options.ConnectionString)
            {
            };
            //Default partitionKeyResolver
            _config = new EntityTableClientConfig<T>
            {
                PartitionKeyResolver = (e) => $"_{ResolvePrimaryKey(e).ToShortHash()}"
            };
            configurator?.Invoke(_config);
            //PrimaryKey required
            _ = _config.PrimaryKeyProp ?? throw new InvalidOperationException($"Primary property is required and must be set");

            _retryPolicy = Policy
                            .Handle<RequestFailedException>(ex => HandleStorageException(options.TableName, _tableService, options.CreateTableIfNotExists, ex))
                            .WaitAndRetryAsync(3, i => TimeSpan.FromSeconds(1));
        }

        public EntityTableClient(EntityTableClientOptions options, EntityTableClientConfig<T> config)
        {
            _options = options;
            _client = new TableClient(options.ConnectionString, options.TableName);

            _ = config.PrimaryKeyProp ?? throw new InvalidOperationException($"Primary property is required and must be set");

            //Default partitionKeyResolver
            _config.PartitionKeyResolver ??= (e) => $"_{ResolvePrimaryKey(e).ToShortHash()}";

            _config = config;
        }

        public async IAsyncEnumerable<IEnumerable<T>> GetAsync(string partition, Action<IQueryCompose<T>> filter = default, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var queryExpr = new FilterExpression<T>();
            //build primaryKey prefix
            var primaryKeyName = ResolvePrimaryKey("");
            //apply implicit filter in the query to exclude duplicated tag entities
            var query = queryExpr
              .Where("PartitionKey").Equal(partition)
              .And("RowKey").GreaterThanOrEqual(primaryKeyName)
              .And("RowKey").LessThan($"{primaryKeyName}~");

            if (filter != null)
            {
                query.And(filter);
            }
            var strQuery = new TableStorageQueryBuilder<T>(queryExpr).Build();

            await foreach (var page in _client.QueryAsync<TableEntity>(filter: strQuery, cancellationToken: cancellationToken).AsPages())
            {
                yield return page.Values.Select(tableEntity => CreateEntityBinderFromTableEntity(tableEntity).UnBind());
            }
        }

        public async Task<T> GetByIdAsync(string partition, object id, CancellationToken cancellationToken = default)
        {
            var rowKey = ResolvePrimaryKey(id);
            try
            {
                var response = await _client.GetEntityAsync<TableEntity>(partition, rowKey, select: new string[] { }, cancellationToken);

                return CreateEntityBinderFromTableEntity(response.Value).UnBind();
            }
            catch (RequestFailedException ex)
            {
                if (ex.ErrorCode == "ResourceNotFound")
                {
                    return null;
                }
                throw;
            }
            catch (Exception ex)
            {
                throw new EntityTableClientException($"An error occured during the request, partition:{partition} rowkey:{rowKey}", ex);
            }
        }

        public async IAsyncEnumerable<IEnumerable<T>> GetByTagAsync<P>(string partition, Expression<Func<T, P>> tagProperty, P tagValue, Action<IQueryCompose<T>> filter = null, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            if (!_config.Tags.ContainsKey(tagProperty.GetPropertyInfo().Name))
            {
                throw new EntityTableClientException($"Property: {tagProperty.GetPropertyInfo().Name}, not tagged");
            }

            var propertyKey = BuildTag(tagProperty.GetPropertyInfo(), tagValue);

            await foreach (var page in RunQueryWithIndexedTagAsync(partition, propertyKey, filter, cancellationToken))
            {
                yield return page;
            }
        }

        public async IAsyncEnumerable<IEnumerable<T>> GetByTagAsync(string partition, string tagProperty, object tagValue, Action<IQueryCompose<T>> filter = null, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            if (!_config.ComputedTags.Contains(tagProperty) && !_config.Tags.ContainsKey(tagProperty))
            {
                throw new EntityTableClientException($"Property: {tagProperty}, not tagged");
            }

            var tag = BuildTag(tagProperty, tagValue);
            await foreach (var page in RunQueryWithIndexedTagAsync(partition, tag, filter, cancellationToken))
            {
                yield return page;
            }
        }

        public Task AddOrReplaceAsync(T entity, CancellationToken cancellationToken = default)
        {
            return UpdateEntity(entity, EntityOperation.AddOrReplace, cancellationToken);
        }

        public Task AddOrMergeAsync(T entity, CancellationToken cancellationToken = default)
        {
            return UpdateEntity(entity, EntityOperation.AddOrMerge, cancellationToken);
        }

        public Task AddAsync(T entity, CancellationToken cancellationToken = default)
        {
            return UpdateEntity(entity, EntityOperation.Add, cancellationToken);
        }

        public Task ReplaceAsync(T entity, CancellationToken cancellationToken = default)
        {
            return UpdateEntity(entity, EntityOperation.Replace, cancellationToken);
        }

        public Task MergeAsync(T entity, CancellationToken cancellationToken = default)
        {
            return UpdateEntity(entity, EntityOperation.Merge, cancellationToken);
        }

        public async Task AddManyAsync(IEnumerable<T> entities, CancellationToken cancellationToken = default)
        {
            var batchedClient = CreateTableBatchClient();
            var cleaner = CreateTableBatchClient();

            foreach (var entity in entities)
            {
                var tableEntities = new List<IEntityBinder<T>>();
                if (cancellationToken.IsCancellationRequested) break;

                var entityBinder = CreateEntityBinderFromEntity(entity);

                 //system metada required to handle implicit tag updates
                entityBinder.Metadata.Add(Deleted_suffix, false);
                BindDynamicProps(entityBinder);
                BindTags(batchedClient, cleaner, entityBinder);
                tableEntities.Add(entityBinder);
                batchedClient.Insert(entityBinder.Bind());
                await batchedClient.AddToTransactionAsync(entityBinder.PartitionKey, entityBinder.RowKey, cancellationToken);
                NotifyChange(entityBinder, EntityOperation.Add);
            }
            await batchedClient.CommitTransactionAsync();
        }

        public async Task DeleteAsync(T entity, CancellationToken cancellationToken = default)
        {
            var entityBinder = CreateEntityBinderFromEntity(entity);
            var batchedClient = CreateTableBatchClient();
            try
            {
                var metadatas = await GetEntityMetadatasAsync(entityBinder.PartitionKey, entityBinder.RowKey, cancellationToken);

                //mark indexed tag soft deleted
                batchedClient.Delete(entityBinder.Bind());
                foreach (var tag in metadatas.Where(m => m.Key.EndsWith(Tag_suffix)))
                {
                    var entityTagBinder = CreateEntityBinderFromEntity(entity, tag.Value.ToString());
                    batchedClient.Delete(entityTagBinder.Bind());
                }
                await batchedClient.ExecuteAsync(cancellationToken);
                NotifyChange(entityBinder, EntityOperation.Delete);
            }
            catch (Exception ex)
            {
                throw new EntityTableClientException($"An error occured during the request, partition:{entityBinder?.PartitionKey} rowkey:{entityBinder?.RowKey}", ex);
            }
        }

        public async Task<IDictionary<string, object>> GetEntityMetadatasAsync(string partitionKey, string rowKey, CancellationToken cancellationToken = default)
        {
            var metadataKeys = _config.Tags.Keys.Union(_config.ComputedTags).Select(k => TagName(k));

            try
            {
                var response = await _retryPolicy.ExecuteAsync(async () => await _client.GetEntityAsync<TableEntity>(partitionKey, rowKey, metadataKeys, cancellationToken));
                var entityBinder = CreateEntityBinderFromTableEntity(response.Value);
                entityBinder.UnBind();
                return entityBinder?.Metadata ?? new Dictionary<string, object>();
            }
            catch (RequestFailedException ex)
            {
                //ignore resource not found exceptions
                if (ex.ErrorCode == "ResourceNotFound")
                {
                    return new Dictionary<string, object>();
                }
                throw new EntityTableClientException($"An error occured during the request, partition:{partitionKey} rowkey:{rowKey}", ex);
            }
            catch (Exception ex)
            {
                throw new EntityTableClientException($"An error occured during the request, partition:{partitionKey} rowkey:{rowKey}", ex);
            }
        }

        public string ResolvePartitionKey(T entity) => _config.PartitionKeyResolver(entity);

        public string ResolvePrimaryKey(T entity)
        {
            return ResolvePrimaryKey(_config.PrimaryKeyProp.GetValue(entity));
        }

        public void AddObserver(string name, IEntityObserver<T> observer)
        {
            _config.Observers.TryAdd(name, observer);
        }

        public void RemoveObserver(string name)
        {
            _config.Observers.TryRemove(name, out var _);
        }

        protected enum BatchOperation
        {
            Insert,
            InsertOrMerge
        }

        protected virtual string ComputeKeyConvention(string name, object value) => $"{name}-{value.ToInvariantString()}";

        protected void NotifyChange(IEntityBinder<T> entityBinder, EntityOperation operation)
        {
            foreach (var observer in _config.Observers)
            {
                observer.Value.OnNext(new EntityOperationContext<T>()
                {
                    Entity = entityBinder.Entity,
                    Metadatas = entityBinder.Metadata,
                    Partition = entityBinder.PartitionKey,
                    TableOperation = operation
                });
            }
        }

        protected void NotifyError(Exception exception)
        {
            foreach (var observer in _config.Observers)
            {
                observer.Value.OnError(exception);
            }
        }

        private async Task UpdateEntity(T entity, EntityOperation operation, CancellationToken cancellationToken = default)
        {
            var client = CreateTableBatchClient();
            var cleaner = CreateTableBatchClient();

            var entityBinder = CreateEntityBinderFromEntity(entity);
            try
            {
                //system metada required to handle implicit tag updates
                entityBinder.Metadata.Add(Deleted_suffix, false);
                BindDynamicProps(entityBinder);
                var existingMetadatas = await GetEntityMetadatasAsync(entityBinder.PartitionKey, entityBinder.RowKey, cancellationToken);

                BindTags(client, cleaner, entityBinder, existingMetadatas);
                switch (operation)
                {
                    case EntityOperation.Add:
                        client.Insert(entityBinder.Bind());
                        break;
                    case EntityOperation.AddOrMerge:
                        client.InsertOrMerge(entityBinder.Bind());
                        break;
                    case EntityOperation.AddOrReplace:
                        client.InsertOrReplace(entityBinder.Bind());
                        break;
                    case EntityOperation.Replace:
                        client.Replace(entityBinder.Bind());
                        break;
                    case EntityOperation.Merge:
                        client.Merge(entityBinder.Bind());
                        break;

                    case EntityOperation.Delete:
                        client.Delete(entityBinder.Bind());
                        break; 
                    default:
                        throw new NotImplementedException($"{operation} operation not supported");

                }
             

                await client.ExecuteAsync(cancellationToken);
                NotifyChange(entityBinder, operation);
                await cleaner.ExecuteAsync(cancellationToken);
            }
            catch (Exception ex)
            {
                throw new EntityTableClientException($"An error occured during the request, partition:{entityBinder?.PartitionKey} rowkey:{entityBinder?.RowKey}", ex);
            }
        }

        private async IAsyncEnumerable<IEnumerable<T>> RunQueryWithIndexedTagAsync(string partition, string tagName, Action<IQueryCompose<T>> query = null, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var queryExpr = new FilterExpression<T>();
            //scope query to given tagName
            var baseQuery = queryExpr
                 .Where("PartitionKey").Equal(partition)
                 .And("RowKey").GreaterThanOrEqual(tagName)
                 .And("RowKey").LessThan($"{tagName}~")
                 .And(Deleted_suffix).Equal(false);
            if (query != null) baseQuery.And(query);

            var strQuery = new TableStorageQueryBuilder<T>(queryExpr).Build();

            await foreach (var page in _client.QueryAsync<TableEntity>(strQuery, cancellationToken: cancellationToken).AsPages())
            {
                yield return page.Values.Select(tableEntity => CreateEntityBinderFromTableEntity(tableEntity).UnBind());
            }
        }

        private string BuildTag(PropertyInfo property, object value) => BuildTag(property.Name, value);

        private string BuildTag(string propertyName, object value)
        {
            if (!_config.ComputedTags.Contains(propertyName) && !_config.Tags.ContainsKey(propertyName)) throw new KeyNotFoundException($"Given tag not configured");
            var strValue = value.ToInvariantString();
            return $"{ComputeKeyConvention(propertyName, strValue)}";
        }

        private string ResolvePrimaryKey(object value) => $"${ComputeKeyConvention(_config.PrimaryKeyProp.Name, value)}";

        private string CreateRowKey(PropertyInfo property, T entity) => $"{ComputeKeyConvention(property.Name, property.GetValue(entity).ToInvariantString())}{ResolvePrimaryKey(entity)}";

        private string CreateRowKey(string key, object value, T entity) => $"{ComputeKeyConvention(key, value.ToInvariantString())}{ResolvePrimaryKey(entity)}";

        private void BindDynamicProps(IEntityBinder<T> tableEntity, bool toDelete = false)
        {
            foreach (var prop in _config.DynamicProps)
            {
                if (toDelete && tableEntity.Metadata.ContainsKey(prop.Key))
                {
                    tableEntity.Metadata.Remove(prop.Key);
                    continue;
                }
                tableEntity.Metadata.Add(prop.Key, prop.Value.Invoke(tableEntity.Entity));
            }
        }

        private void BindTags(TableBatchClient client, TableBatchClient cleaner, IEntityBinder<T> tableEntity, IDictionary<string, object> existingMetadatas = null)
        {
            var tags = new Dictionary<string, object>();
            foreach (var propInfo in _config.Tags)
            {
                var tagValue = CreateRowKey(propInfo.Value, tableEntity.Entity);
                var entityBinder = CreateEntityBinderFromEntity(tableEntity.Entity, tagValue);
                tableEntity.CopyMetadataTo(entityBinder);
                client.InsertOrReplace(entityBinder.Bind());
                tags.Add(TagName(propInfo.Key), tagValue);
            }
            foreach (var tagPrefix in _config.ComputedTags)
            {
                var tagValue = CreateRowKey(tagPrefix, tableEntity.Metadata[$"{tagPrefix}"], tableEntity.Entity);
                var entityBinder = CreateEntityBinderFromEntity(tableEntity.Entity, tagValue);
                tableEntity.CopyMetadataTo(entityBinder);
                client.InsertOrReplace(entityBinder.Bind());
                tags.Add(TagName(tagPrefix), tagValue);
            }
            if (existingMetadatas != null)
            {
                //cleanup old indexed tags
                foreach (var metadata in existingMetadatas.Where(m => !tags.ContainsValue(m.Value) && m.Key.EndsWith(Tag_suffix)))
                {
                    var tagValue = metadata.Value.ToString();
                    var entityBinder = CreateEntityBinderFromEntity(tableEntity.Entity, tagValue);
                    //mark tag deleted
                    entityBinder.Metadata.Add(Deleted_suffix, true);
                    client.InsertOrReplace(entityBinder.Bind());
                    cleaner.Delete(entityBinder.Bind());
                }
            }

            //attach tag keys to main entity
            foreach (var tag in tags)
                tableEntity.Metadata.Add(tag);
        }

        private IEntityBinder<T> CreateEntityBinderFromEntity(T entity, string customRowKey = null)
          => new EntityTableBinder<T>(entity, ResolvePartitionKey(entity), customRowKey ?? ResolvePrimaryKey(entity));

        private IEntityBinder<T> CreateEntityBinderFromTableEntity(TableEntity tableEntity)
            => new EntityTableBinder<T>(tableEntity);

        private TableBatchClient CreateTableBatchClient()
        {
            return new TableBatchClient(
                new TableBatchClientOptions()
                {
                    TableName = _options.TableName,
                    ConnectionString = _options.ConnectionString,
                    MaxParallelTasks = _options.MaxParallelTasks
                }, _retryPolicy
                )
            { };
        }

        private static bool HandleStorageException(string tableName, TableServiceClient tableService, bool createTableIfNotExists, RequestFailedException requestFailedException)
        {
            if (createTableIfNotExists && (requestFailedException?.ErrorCode == "TableNotFound"))
            {
                tableService.CreateTableIfNotExists(tableName);
                return true;
            }
        
            if (requestFailedException?.ErrorCode == "TableBeingDeleted" || requestFailedException?.ErrorCode == "OperationTimedOut")
            { 
                return true;
            }
           
            return false;
        }

        public Task DropTableAsync(CancellationToken cancellationToken = default)
        {
            return _client.DeleteAsync(cancellationToken);
        }
    }
}