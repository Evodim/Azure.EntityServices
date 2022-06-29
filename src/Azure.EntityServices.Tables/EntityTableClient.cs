using Azure.Data.Tables;
using Azure.EntityServices.Queries;
using Azure.EntityServices.Tables.Core;
using Azure.EntityServices.Tables.Extensions;
using Polly;
using Polly.Retry;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Azure.EntityServices.Tables
{
    /// <summary>
    /// Client to manage entities in azure Tables
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class EntityTableClient<T> : IEntityTableClient<T>
    where T : class, new()
    {
       
        protected const string IndexedTagSuffix = "_indexed_tag_";
        protected readonly Func<string, string> TagName = (tagName) => $"{tagName}{IndexedTagSuffix}";

        private EntityTableClientConfig<T> _config;
        private EntityTableClientOptions _options;
        private AsyncRetryPolicy _retryPolicy;
        private TableClient _client;
        private TableServiceClient _tableService;
      

        public EntityTableClient(EntityTableClientOptions options, Action<EntityTableClientConfig<T>> configurator)
        {
            _ = options ?? throw new ArgumentNullException(nameof(options));
            _config = new();
            configurator?.Invoke(_config);
            Configure(options, _config);
        }

        public EntityTableClient(EntityTableClientOptions options, EntityTableClientConfig<T> config)
        {
            _ = options ?? throw new ArgumentNullException(nameof(options));
            _ = config ?? throw new ArgumentNullException(nameof(config));

            Configure(options,config);
        }
        private void Configure(EntityTableClientOptions options, EntityTableClientConfig<T> config)
        {
            _options = options;
            _config= config;
            _config.PartitionKeyResolver ??= (e) => $"_{ResolvePrimaryKey(e).ToShortHash()}";
            _client = new TableClient(options.ConnectionString, options.TableName);
            _tableService = new TableServiceClient(options.ConnectionString)
            {
            };
        

            //PrimaryKey required
            _ = _config.PrimaryKeyProp ?? throw new InvalidOperationException($"Primary property is required and must be set");

            //If tag was added, you must enable indexed tag feature in config
            if (!_options.EnableIndexedTagSupport && (_config.Tags.Any() || _config.ComputedTags.Any()))
            {
                throw new InvalidOperationException($"You must set EnableIndexedTagSupport option in order to use indexed Tags");
            }


            _retryPolicy = Policy
                            .Handle<RequestFailedException>(ex => HandleStorageException(options.TableName, _tableService, options.CreateTableIfNotExists, ex))
                            .WaitAndRetryAsync(3, i => TimeSpan.FromSeconds(1));
            _config = config;

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

        public async IAsyncEnumerable<IEnumerable<T>> GetByTagAsync(Action<ITagQuery<T>> filter, [EnumeratorCancellation] CancellationToken cancellationToken = default)

        {
            await foreach (var page in QueryEntityByTagAsync(filter, null, null, cancellationToken))
            {
                yield return page.Values.Select(tableEntity => CreateEntityBinderFromTableEntity(tableEntity).UnBind());
            }
        }

        public async Task<EntityPage<T>> GetByTagPagedAsync(Action<ITagQuery<T>> filter, int? maxPerPage = null, string nextPageToken = null, CancellationToken cancellationToken = default)
        {
            var pageEnumerator =
              QueryEntityByTagAsync(filter, maxPerPage, nextPageToken, cancellationToken)
                       .GetAsyncEnumerator(cancellationToken);
            try
            {
                await pageEnumerator.MoveNextAsync();
                return new EntityPage<T>(pageEnumerator.Current.Values.Select(tableEntity => CreateEntityBinderFromTableEntity(tableEntity).UnBind()), pageEnumerator.Current.ContinuationToken);
            }
            finally
            {
                await pageEnumerator.DisposeAsync();
            }
        }

        public async IAsyncEnumerable<IEnumerable<T>> GetAsync(Action<IQuery<T>> filter = default, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await foreach (var page in QueryEntityAsync(filter, null, null, cancellationToken))
            {
                yield return page.Values.Select(tableEntity => CreateEntityBinderFromTableEntity(tableEntity).UnBind());
            }
        }

        public async Task<EntityPage<T>> GetPagedAsync(
            Action<IQuery<T>> filter = default,
            int? maxPerPage = null,
            string nextPageToken = null,
            CancellationToken cancellationToken = default)
        {
            
                var pageEnumerator =
                    QueryEntityAsync(filter, maxPerPage, nextPageToken, cancellationToken)
                             .GetAsyncEnumerator(cancellationToken);
            try
            {
                await pageEnumerator.MoveNextAsync(); 
                return new EntityPage<T>(pageEnumerator.Current.Values.Select(tableEntity => CreateEntityBinderFromTableEntity(tableEntity).UnBind()), pageEnumerator.Current.ContinuationToken);
            }
            finally
            {
                await pageEnumerator.DisposeAsync();
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

        public Task AddManyAsync(IEnumerable<T> entities, CancellationToken cancellationToken = default)
        {
            return ApplyBatchOperations(EntityOperation.Add, entities, cancellationToken);
        }
        public Task AddOrReplaceManyAsync(IEnumerable<T> entities, CancellationToken cancellationToken = default)
        {
            return ApplyBatchOperations(EntityOperation.AddOrReplace, entities, cancellationToken);
        }
        public Task AddOrMergeManyAsync(IEnumerable<T> entities, CancellationToken cancellationToken = default)
        {
            return ApplyBatchOperations(EntityOperation.AddOrMerge, entities, cancellationToken);
        }

        private async Task ApplyBatchOperations(EntityOperation operation,IEnumerable<T> entities, CancellationToken cancellationToken)
        {
            var batchedClient = CreateTableBatchClient();
            var cleaner = CreateTableBatchClient();

            if (_options.EnableIndexedTagSupport && operation != EntityOperation.Add)
            {
                throw new NotSupportedException($"Operation {operation} not supported when indexed tag support was enabled, please check EntityTableClientOptions");
            }

            foreach (var entity in entities)
            {
                var tableEntities = new List<IEntityBinder<T>>();
                if (cancellationToken.IsCancellationRequested) break;

                var binder = CreateEntityBinderFromEntity(entity);
                //system metada required to cleanup old tags
                binder.Metadata.Add(EntitytableConstants.DeletedTag, false);
                binder.BindDynamicProps(_config.DynamicProps);
                
                UpdateTags(batchedClient, cleaner, binder);
                 
                tableEntities.Add(binder);
                switch(operation)
                {
                    case EntityOperation.Add:
                        batchedClient.Insert(binder.Bind());
                        break;
                    case EntityOperation.AddOrMerge:
                        batchedClient.InsertOrMerge(binder.Bind());
                        break; 
                    case EntityOperation.AddOrReplace:
                        batchedClient.InsertOrReplace(binder.Bind());
                        break;

                }
                
                await batchedClient.SubmitToPipelineAsync(binder.PartitionKey, cancellationToken);
                NotifyChange(binder, operation);
            }
            await batchedClient.CommitTransactionAsync();
        }

        public async Task<long> UpdateManyAsync(Action<T> updateAction, Action<IQuery<T>> filter = default, CancellationToken cancellationToken = default)
        {
            long count = 0;
            var batchedClient = CreateTableBatchClient();
            var cleaner = CreateTableBatchClient();

            await foreach (var page in QueryEntityAsync(filter, null, null, cancellationToken))
            {
                foreach (var tableEntity in page.Values)
                {

                    if (cancellationToken.IsCancellationRequested) break;

                    var binder = CreateEntityBinderFromTableEntity(tableEntity);
                    var entity = binder.UnBind();
                    updateAction.Invoke(entity);
                    var existingMetadata = binder.Metadata.ToDictionary(d => d.Key, d => d.Value);
                    binder.Metadata.Clear();
                    //system metada required to cleanup old tags
                    binder.Metadata.Add(EntitytableConstants.DeletedTag, false);
                    binder.BindDynamicProps(_config.DynamicProps);

                    UpdateTags(batchedClient, cleaner, binder, existingMetadata);
                    batchedClient.InsertOrMerge(binder.Bind());
                    await batchedClient.SubmitToPipelineAsync(binder.PartitionKey, cancellationToken); 
                    await cleaner.SubmitToPipelineAsync(binder.PartitionKey, cancellationToken);
                    count++;
                }
#if DEBUG
                System.Diagnostics.Debug.WriteLine("Entities updated {0}", count);
#endif
            }
            await batchedClient.CommitTransactionAsync();
            await cleaner.CommitTransactionAsync();
            return count;
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
                foreach (var tag in metadatas.Where(m => m.Key.EndsWith(IndexedTagSuffix)))
                {
                    var entityTagBinder = CreateEntityBinderFromEntity(entity, tag.Value.ToString());
                    batchedClient.Delete(entityTagBinder.Bind());
                }
                await batchedClient.SubmitAllAsync(cancellationToken);
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
                //return null reference when existing entity was not found
                if (ex.ErrorCode == "ResourceNotFound")
                {
                    return default;
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
            return $"${TableQueryHelper.ToRowKey(_config.PrimaryKeyProp.Name, _config.PrimaryKeyProp.GetValue(entity))}";
        }

        public string ResolvePrimaryKey(object value)
        {
            return $"${TableQueryHelper.ToRowKey(_config.PrimaryKeyProp.Name, value)}";
        }

        public void AddObserver(string name, IEntityObserver<T> observer)
        {
            _config.Observers.TryAdd(name, observer);
        }

        public void RemoveObserver(string name)
        {
            _config.Observers.TryRemove(name, out var _);
        }

        public Task DropTableAsync(CancellationToken cancellationToken = default)
        {
            return _client.DeleteAsync(cancellationToken);
        }
        public Task CreateTableAsync(CancellationToken cancellationToken = default)
        {
            return _client.CreateIfNotExistsAsync(cancellationToken);
        }
        protected enum BatchOperation
        {
            Insert,
            InsertOrMerge
        }

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
                //system metada required to cleanup old tags
                entityBinder.Metadata.Add(EntitytableConstants.DeletedTag, false);
                entityBinder.BindDynamicProps(_config.DynamicProps);

                //we don't need to retrieve existing entity metadata for add operation
                var existingMetadatas = (operation != EntityOperation.Add && _options.EnableIndexedTagSupport) ?
                        await GetEntityMetadatasAsync(entityBinder.PartitionKey, entityBinder.RowKey, cancellationToken)
                        : null;
                UpdateTags(client, cleaner, entityBinder, existingMetadatas);
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
                await client.SubmitAllAsync(cancellationToken);
                NotifyChange(entityBinder, operation);
                await cleaner.SubmitAllAsync(cancellationToken);
            }
            catch (Exception ex)
            {
                throw new EntityTableClientException($"An error occured during the request, partition:{entityBinder?.PartitionKey} rowkey:{entityBinder?.RowKey}", ex);
            }
        }

        private string CreateRowKey(PropertyInfo property, T entity) => $"{TableQueryHelper.ToRowKey(property.Name, property.GetValue(entity))}{ResolvePrimaryKey(entity)}";

        private string CreateRowKey(string key, object value, T entity) => $"{TableQueryHelper.ToRowKey(key, value)}{ResolvePrimaryKey(entity)}";

        private void UpdateTags(TableBatchClient client, TableBatchClient cleaner, IEntityBinder<T> tableEntity, IDictionary<string, object> existingMetadatas = null)
        {
            var tags = new Dictionary<string, object>();
            foreach (var propInfo in _config.Tags)
            {
                var tagValue = CreateRowKey(propInfo.Value, tableEntity.Entity);
                var entityBinder = CreateEntityBinderFromEntity(tableEntity.Entity, tagValue);
                tableEntity.CopyMetadataTo(entityBinder);
                client.InsertOrReplace(entityBinder.Bind());
                tags.AddOrUpdate(TagName(propInfo.Key), tagValue);
            }
            foreach (var tagPrefix in _config.ComputedTags)
            {
                var tagValue = CreateRowKey(tagPrefix, tableEntity.Metadata[$"{tagPrefix}"], tableEntity.Entity);
                var entityBinder = CreateEntityBinderFromEntity(tableEntity.Entity, tagValue);
                tableEntity.CopyMetadataTo(entityBinder);
                client.InsertOrReplace(entityBinder.Bind());
                tags.AddOrUpdate(TagName(tagPrefix), tagValue);
            }
            if (existingMetadatas != null)
            {
                //cleanup old indexed tags
                foreach (var metadata in existingMetadatas.Where(m => !tags.ContainsValue(m.Value) && m.Key.EndsWith(IndexedTagSuffix)))
                {
                    var tagValue = metadata.Value.ToString();
                    var entityBinder = CreateEntityBinderFromEntity(tableEntity.Entity, tagValue);
                    //mark tag deleted
                    var table = entityBinder.Bind();
                    entityBinder.Metadata.Add(EntitytableConstants.DeletedTag, true);
                    client.InsertOrReplace(table);
                    cleaner.Delete(table);
                }
            }

            //attach tag keys to main entity
            foreach (var tag in tags)
            {
                tableEntity.Metadata.AddOrUpdate(tag);
            }
        }

        private IEntityBinder<T> CreateEntityBinderFromEntity(T entity, string customRowKey = null)
          => new TableEntityBinder<T>(entity, ResolvePartitionKey(entity), customRowKey ?? ResolvePrimaryKey(entity), _config.IgnoredProps);

        private IEntityBinder<T> CreateEntityBinderFromTableEntity(TableEntity tableEntity)
            => new TableEntityBinder<T>(tableEntity, _config.IgnoredProps);

        private TableBatchClient CreateTableBatchClient()
        {
            return new TableBatchClient(
                new TableBatchClientOptions()
                {
                    TableName = _options.TableName,
                    ConnectionString = _options.ConnectionString,
                    MaxItemInBatch = _options.MaxItemToGroup,
                    MaxItemInTransaction = _options.MaxOperationPerTransaction,
                    MaxParallelTasks = _options.MaxParallelTransactions == -1 ? Environment.ProcessorCount : _options.MaxParallelTransactions
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

        private IAsyncEnumerable<Page<TableEntity>> QueryEntityAsync(Action<IQuery<T>> filter, int? maxPerPage, string nextPageToken, CancellationToken cancellationToken)
        {
            var query = new FilterExpression<T>();
            //build primaryKey prefix
            var primaryKeyName = ResolvePrimaryKey("");

            //add group expression to scope the filter with non tagged rows
            query
                  .WhereRowKey()
                  .GreaterThanOrEqual(primaryKeyName)
                  .AndRowKey()
                  .LessThan($"{primaryKeyName}~")
                  .And(filter);

            var strQuery = new TableStorageQueryBuilder<T>(query).Build();

            return _client.QueryAsync<TableEntity>(filter: strQuery, cancellationToken: cancellationToken, maxPerPage: maxPerPage).AsPages(nextPageToken);
        }

        private IAsyncEnumerable<Page<TableEntity>> QueryEntityByTagAsync(Action<ITagQuery<T>> filter, int? maxPerPage, string nextPageToken, CancellationToken cancellationToken)
        {
            var query = new TagFilterExpression<T>("");
            filter.Invoke(query);
            var strQuery = new TableStorageQueryBuilder<T>(query).Build();

            return _client.QueryAsync<TableEntity>(filter: strQuery, cancellationToken: cancellationToken, maxPerPage: maxPerPage).AsPages(nextPageToken);
        }

        public Task DeleteManyAsync(IEnumerable<T> entities, CancellationToken cancellationToken = default)
        {
            return ApplyBatchOperations(EntityOperation.Delete, entities, cancellationToken);
        }
    }

    public record struct EntityPage<T>(IEnumerable<T> Entities, string ContinuationToken)
    {
        public static implicit operator (IEnumerable<T>, string ContinuationToken)(EntityPage<T> value)
        {
            return (value.Entities, value.ContinuationToken);
        }

        public static implicit operator EntityPage<T>((IEnumerable<T>, string ContinuationToken) value)
        {
            return new EntityPage<T>(value.Item1, value.ContinuationToken);
        }
    }
}