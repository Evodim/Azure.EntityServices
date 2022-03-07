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
        protected const string DeletedTagSuffix = $"_deleted{TagSuffix}";
        protected const string TagSuffix = "_tag_";
        protected readonly Func<string, string> TagName = (tagName) => $"{tagName}{TagSuffix}";

        private readonly EntityTableClientConfig<T> _config;
        private readonly EntityTableClientOptions _options;
        private readonly TableClient _client;
        private readonly TableServiceClient _tableService;
        private readonly AsyncRetryPolicy _retryPolicy;

        public EntityTableClient(EntityTableClientOptions options, Action<EntityTableClientConfig<T>> configurator)
        {
            _ = options ?? throw new ArgumentNullException(nameof(options));

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
            _ = options ?? throw new ArgumentNullException(nameof(options));

            _options = options;
            _client = new TableClient(options.ConnectionString, options.TableName);

            _ = config.PrimaryKeyProp ?? throw new InvalidOperationException($"Primary property is required and must be set");

            //Default partitionKeyResolver
            _config.PartitionKeyResolver ??= (e) => $"_{ResolvePrimaryKey(e).ToShortHash()}";

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
            await pageEnumerator.MoveNextAsync();
            return new EntityPage<T>(pageEnumerator.Current.Values.Select(tableEntity => CreateEntityBinderFromTableEntity(tableEntity).UnBind()), pageEnumerator.Current.ContinuationToken);
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
            await pageEnumerator.MoveNextAsync();
            return new EntityPage<T>(pageEnumerator.Current.Values.Select(tableEntity => CreateEntityBinderFromTableEntity(tableEntity).UnBind()), pageEnumerator.Current.ContinuationToken);
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

                var binder = CreateEntityBinderFromEntity(entity);
                //system metada required to cleanup old tags
                binder.Metadata.Add(DeletedTagSuffix, false);
                binder.BindDynamicProps(_config.DynamicProps);
                UpdateTags(batchedClient, cleaner, binder);
                tableEntities.Add(binder);
                batchedClient.Insert(binder.Bind());
                await batchedClient.SubmitTransactionAsync(binder.PartitionKey, cancellationToken);
                NotifyChange(binder, EntityOperation.Add);
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

                    binder.Metadata.Add(DeletedTagSuffix, false);
                    binder.BindDynamicProps(_config.DynamicProps);
                    UpdateTags(batchedClient, cleaner, binder, binder.Metadata);
                    batchedClient.Replace(tableEntity);

                    await batchedClient.SubmitTransactionAsync(binder.PartitionKey, cancellationToken);
                    await cleaner.SubmitTransactionAsync(binder.PartitionKey, cancellationToken);
                    NotifyChange(binder, EntityOperation.Add);
                    count++;
                }
            }
            await batchedClient.CommitTransactionAsync();
            //await cleaner.CommitTransactionAsync();
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
                foreach (var tag in metadatas.Where(m => m.Key.EndsWith(TagSuffix)))
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
                entityBinder.Metadata.Add(DeletedTagSuffix, false);
                entityBinder.BindDynamicProps(_config.DynamicProps);

                //we don't need to retrieve existing entity metadata for add operation
                var existingMetadatas = (operation != EntityOperation.Add) ?
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
                foreach (var metadata in existingMetadatas.Where(m => !tags.ContainsValue(m.Value) && m.Key.EndsWith(TagSuffix)))
                {
                    var tagValue = metadata.Value.ToString();
                    var entityBinder = CreateEntityBinderFromEntity(tableEntity.Entity, tagValue);
                    //mark tag deleted
                    entityBinder.Metadata.Add(DeletedTagSuffix, true);
                    client.InsertOrReplace(entityBinder.Bind());
                    cleaner.Delete(entityBinder.Bind());
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
                    MaxParallelTasks = _options.MaxParallelTransactions
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