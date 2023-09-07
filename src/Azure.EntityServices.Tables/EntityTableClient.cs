using Azure.Data.Tables;
using Azure.EntityServices.Queries;
using Azure.EntityServices.Tables.Core;
using Azure.EntityServices.Tables.Extensions;
using Polly;
using Polly.Retry;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Azure.EntityServices.Tables
{
    /// <summary>
    /// Client to manage entities in azure Tables
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class EntityTableClient<T> : IEntityTableClient<T>, IObservableEntityTableClient<T>
    where T : class, new()
    {
        protected async Task NotifyChangeAsync(IEnumerable<EntityContext<T>> context)
        {
            foreach (var observer in _observerInstances)
            {
                await observer.OnNextAsync(context);
            }
        }

        protected async Task NotifyExceptionAsync(Exception ex)
        {
            foreach (var observer in _observerInstances)
            {
                await observer.OnErrorAsync(ex);
            }
        }

        protected async Task NotifyCompleteAsync()
        {
            foreach (var observer in _observerInstances)
            {
                await observer.OnCompletedAsync();
            }
        }

        private EntityTableClientConfig<T> _config;

        private EntityTableClientOptions _options;

        private RetryPolicy _retryPolicy;

        private AsyncRetryPolicy _asyncRetryPolicy;

        private TableClient _client;

        private TableClient _configuredClient => _client ?? throw new InvalidOperationException("EntityTableClient was not configured");

        private readonly TableServiceClient _tableServiceClient;
        private TableEntityAdapter<T> _entityAdapter;

        private Func<IEnumerable<TableTransactionAction>, Task> _pipelineObserver;

        private IEnumerable<string> _indextedTags;
        private IList<string> _indextedTagsWithKeys;
        private IEnumerable<IEntityObserver<T>> _observerInstances;

        private Func<EntityTransactionGroup, Task<EntityTransactionGroup>> _pipelinePreProcessor;

        private EntityKeyBuilder<T> _entityKeyBuilder;

        private TableBatchClient CreateTableBatchClient()
        {
            return new TableBatchClient(
                _tableServiceClient,
                new TableBatchClientOptions()
                {
                    TableName = _options.TableName,
                    MaxItemInBatch = _options.MaxItemToGroup,
                    MaxItemInTransaction = _options.MaxOperationPerTransaction,
                    MaxParallelTasks = _options.MaxParallelTransactions == -1 ? Environment.ProcessorCount : _options.MaxParallelTransactions
                }, _asyncRetryPolicy,
                   _pipelinePreProcessor,
                   _pipelineObserver

                )
            { };
        }

        private static bool HandleStorageException(string tableName, TableServiceClient tableService, bool createTableIfNotExists, RequestFailedException requestFailedException)
        {
            try
            {
                if (createTableIfNotExists && (requestFailedException?.ErrorCode == "TableNotFound"))
                {
                    tableService.CreateTableIfNotExists(tableName);
                    return true;
                }

                if (requestFailedException?.ErrorCode == "TableBeingDeleted" ||
                    requestFailedException?.ErrorCode == "OperationTimedOut" ||
                    requestFailedException?.ErrorCode == "TooManyRequests"
                    )
                {
                    return true;
                }
            }
            catch (RequestFailedException ex)
            {
                if (ex?.ErrorCode == "TableBeingDeleted" ||
                   ex?.ErrorCode == "OperationTimedOut" ||
                   ex?.ErrorCode == "TooManyRequests"
                   )
                {
                    return true;
                }
            }
            return false;
        }

        private IAsyncEnumerable<Page<TableEntity>> QueryTableEntities(Action<IQuery<T>> filter, int? maxPerPage, string nextPageToken, CancellationToken cancellationToken, bool? iterateOnly = false)
        {
            var query = new TagFilterExpression<T>();
            filter?.Invoke(query);

            if (string.IsNullOrEmpty(query.TagName))
            {
                query = new TagFilterExpression<T>();
                query
                      .IgnoreTags()
                      .And(filter);
            }

            var strQuery = new TableStorageQueryBuilder<T>(query).Build();

            return _retryPolicy.Execute(() => _configuredClient
            .QueryAsync<TableEntity>(filter: string.IsNullOrWhiteSpace(strQuery) ? null : strQuery, select: iterateOnly == true ? new string[] { "PartitionKey", "RowKey" } : null,
                                     cancellationToken: cancellationToken,
                                     maxPerPage: maxPerPage

                                     )
            .AsPages(nextPageToken));
        }

        private async IAsyncEnumerable<EntityPage<T>> QueryEntities(
            Action<IQuery<T>> filter,
            int? maxPerPage,
            string nextPageToken,
            [EnumeratorCancellation] CancellationToken cancellationToken,
            bool? iterateOnly = false)
        {
            var iteratedCount = 0;
            await foreach (var page in QueryTableEntities(filter, maxPerPage, nextPageToken, cancellationToken, iterateOnly))
            {
                yield return new EntityPage<T>(
                    page.Values.Select(tableEntity => _entityAdapter.FromEntityModel(tableEntity)),
                    iteratedCount += page.Values.Count,
                    string.IsNullOrWhiteSpace(page.ContinuationToken),
                    page.ContinuationToken);
            }
        }

        public async Task<EntityPage<T>> GetPagedAsync(
          Action<IQuery<T>> filter = default,
          int? iteratedCount = null,
          int? maxPerPage = null,
          string nextPageToken = null,
          CancellationToken cancellationToken = default
          )
        {
            var continuationToken = nextPageToken;
            IAsyncEnumerator<EntityPage<T>> pageEnumerator = null;

            try
            {
                //Create a new iterator after skipping entities to return next available entities
                pageEnumerator = QueryEntities(filter, maxPerPage, continuationToken, cancellationToken)
                         .GetAsyncEnumerator(cancellationToken);

                await pageEnumerator.MoveNextAsync();

                var currentCount = (iteratedCount ?? 0) + pageEnumerator.Current.IteratedCount;

                return pageEnumerator.Current with { IteratedCount = currentCount };
            }
            finally
            {
                if (pageEnumerator != null)
                {
                    await pageEnumerator.DisposeAsync();
                }
            }
        }

        private async Task UpdateEntity(T entity, EntityOperation operation, CancellationToken cancellationToken = default)
        {
            var client = CreateTableBatchClient();

            try
            {
                var tableEntity = _entityAdapter.ToEntityModel(entity);
                switch (operation)
                {
                    case EntityOperation.Add:
                        client.Insert(tableEntity);
                        break;

                    case EntityOperation.AddOrMerge:
                        client.InsertOrMerge(tableEntity);
                        break;

                    case EntityOperation.AddOrReplace:
                        client.InsertOrReplace(tableEntity);
                        break;

                    case EntityOperation.Replace:
                        client.Replace(tableEntity);
                        break;

                    case EntityOperation.Merge:
                        client.Merge(tableEntity);
                        break;

                    case EntityOperation.Delete:
                        client.Delete(tableEntity);
                        break;

                    default:
                        throw new NotImplementedException($"{operation} operation not supported");
                }
                await client.SubmitToStorageAsync(cancellationToken);
                await NotifyCompleteAsync();
            }
            catch (Exception ex)
            {
                await NotifyExceptionAsync(ex);
                throw new EntityTableClientException($"An error occured during the request, partition:{_entityKeyBuilder.ResolvePartitionKey(entity)} rowkey:{_entityKeyBuilder.ResolvePrimaryKey(entity)}", ex);
            }
        }

        public EntityTableClient()
        {
        }

        internal EntityTableClient(TableServiceClient tableService)
        {
            _tableServiceClient = tableService;
        }

        public EntityTableClient<T> Configure(EntityTableClientOptions options, EntityTableClientConfig<T> config)
        {
            _ = options ?? throw new ArgumentNullException(nameof(options));
            _ = config ?? throw new ArgumentNullException(nameof(config));
            _options = options;

            if (string.IsNullOrWhiteSpace(_options.TableName))
            {
                throw new ArgumentNullException(nameof(_options.TableName));
            }

            _config = config;
            _config.RowKeyResolver ??= (e) => _config.RowKeyProp.GetValue(e);
            _config.PartitionKeyResolver ??= (e) => $"_{_config.RowKeyResolver(e).ToInvariantString().ToShortHash()}";
            _entityKeyBuilder = new EntityKeyBuilder<T>(_config.PartitionKeyResolver, _config.RowKeyResolver);

            _client ??= _tableServiceClient.GetTableClient(options.TableName);
            _observerInstances = _config.Observers.Select(o => o.Value.Invoke()).ToList();

            //PrimaryKey required
            _ = _config.RowKeyResolver ?? throw new InvalidOperationException($"at least one of RowKeyResolver or PrimaryKeyResolver was required and must be set");

            var basePolicy = Policy.Handle<RequestFailedException>(ex => HandleStorageException(options.TableName, _tableServiceClient, options.CreateTableIfNotExists, ex));

            _retryPolicy = basePolicy
                                .WaitAndRetry(5, i => TimeSpan.FromSeconds(2 * i));
            _asyncRetryPolicy = basePolicy
                                .WaitAndRetryAsync(5, i => TimeSpan.FromSeconds(2 * i));
            _config = config;

            _pipelineObserver = transactions => NotifyChangeAsync(transactions.Select(
                transaction => new EntityContext<T>(
                    transaction.Entity.PartitionKey,
                    transaction.Entity.RowKey,
                    new TableEntityDataReader<T>(transaction.Entity as TableEntity, _entityAdapter),
                    transaction.ActionType.MapToEntityOperation())));

            _indextedTags =
              _config.Tags.Keys
              .Union(_config.ComputedTags)
              .Select(t => _entityKeyBuilder.CreateTagName(t))
              .Where(t => t.EndsWith(_entityKeyBuilder.IndexedTagSuffix));

            _indextedTagsWithKeys = _indextedTags.ToList();
            _indextedTagsWithKeys.Add("PartitionKey");
            _indextedTagsWithKeys.Add("RowKey");

            _pipelinePreProcessor = async transaction =>
            {
                var newEntity = transaction.Actions.FirstOrDefault()?.Entity as TableEntity;

                var existingEntity = default(NullableResponse<TableEntity>);
                var entityAction = transaction.Actions.First().ActionType;
                if (entityAction != TableTransactionActionType.Add && _options.HandleTagMutation)
                {
                    existingEntity = await _configuredClient.GetEntityIfExistsAsync<TableEntity>(newEntity.PartitionKey, newEntity.RowKey, _indextedTagsWithKeys);
                }
                //duplicate entity per tags in same partition
                foreach (var tag in _indextedTags)
                {
                    //handle tag mutation
                    if (existingEntity != null &&
                        existingEntity.HasValue &&
                        existingEntity.Value.TryGetValue(tag, out var existingtagValue) &&
                        newEntity[tag] is string tagValue && tagValue != existingtagValue as string
                        )
                    {
                        //tag value was changed, existing tag entity needs must be deleted
                        var oldEntityTag = new TableEntity(existingEntity.Value);
                        var oldEntityTagAction = new TableTransactionAction(TableTransactionActionType.Delete, oldEntityTag);
                        oldEntityTagAction.Entity.RowKey = existingtagValue as string;
                        transaction.Actions.Add(oldEntityTagAction);

                        //no needs to add new tag entity
                        if (entityAction == TableTransactionActionType.Delete)
                        {
                            continue;
                        }
                    }

                    var newEntityTagAction =
                    new TableTransactionAction(entityAction == TableTransactionActionType.Delete ?
                    TableTransactionActionType.Delete :
                    TableTransactionActionType.UpsertReplace, new TableEntity(newEntity));

                    newEntityTagAction.Entity.RowKey = newEntity[tag] as string;
                    transaction.Actions.Add(newEntityTagAction);
                }
                return transaction;
            };

            _entityAdapter = new TableEntityAdapter<T>(
            _entityKeyBuilder,
            _config.ComputedProps,
            _config.Tags,
            _config.ComputedTags,
            _config.IgnoredProps,
            _options.SerializerOptions);

            return this;
        }

        public async Task<T> GetByIdAsync(string partition, object id, CancellationToken cancellationToken = default)
        {
            var rowKey = _entityKeyBuilder.ResolvePrimaryKey(id);
            try
            {
                var response = await _asyncRetryPolicy.ExecuteAsync(async () => await _configuredClient.GetEntityIfExistsAsync<TableEntity>(partition.EscapeDisallowedKeyValue(), rowKey, select: new string[] { }, cancellationToken));
                if (!response.HasValue)
                {
                    return null;
                }
                return _entityAdapter.FromEntityModel(response.Value);
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

        public async IAsyncEnumerable<IEnumerable<T>> GetAsync(Action<IQuery<T>> filter = default,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await foreach (var page in QueryEntities(filter, null, null, cancellationToken))
            {
                yield return page.Entities;
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

        public async Task<long> UpdateManyAsync(Action<T> updateAction, Action<IQuery<T>> filter = default, CancellationToken cancellationToken = default)
        {
            long count = 0;
            var batchedClient = CreateTableBatchClient();

            await foreach (var entityPage in QueryEntities(filter, null, null, cancellationToken))
            {
                foreach (var entity in entityPage.Entities)
                {
                    if (cancellationToken.IsCancellationRequested) break;

                    updateAction.Invoke(entity);

                    batchedClient.InsertOrMerge(_entityAdapter.ToEntityModel(entity));

                    await batchedClient.SubmitToPipelineAsync(_entityKeyBuilder.ResolvePartitionKey(entity), cancellationToken);
                    count++;
                }
#if DEBUG
                System.Diagnostics.Debug.WriteLine("Entities updated {0}", count);
#endif
            }
            await batchedClient.CommitTransactionAsync();
            await NotifyCompleteAsync();

            return count;
        }

        public void AddObserver(string name, Func<IEntityObserver<T>> observerFactory)
        {
            _config.Observers.TryAdd(name, observerFactory);
        }

        public void RemoveObserver(string name)
        {
            _config.Observers.TryRemove(name, out var _);
        }

        public Task DropTableAsync(CancellationToken cancellationToken = default)
        {
            return _configuredClient.DeleteAsync(cancellationToken);
        }

        public Task CreateTableAsync(CancellationToken cancellationToken = default)
        {
            return _configuredClient.CreateIfNotExistsAsync(cancellationToken);
        }

        public Task DeleteManyAsync(IEnumerable<T> entities, CancellationToken cancellationToken = default)
        {
            return ApplyBatchOperations(EntityOperation.Delete, entities, cancellationToken);
        }

        public Task<bool> DeleteAsync(T entity, CancellationToken cancellationToken = default)
        {
            var partitionKey = _entityKeyBuilder.ResolvePartitionKey(entity);
            var rowKey = _entityKeyBuilder.ResolvePrimaryKey(entity);

            return DeleteByIdAsync(partitionKey, rowKey, cancellationToken);
        }

        public async Task<bool> DeleteByIdAsync(string partition, object id, CancellationToken cancellationToken = default)
        {
            var entity = await GetByIdAsync(partition, id, cancellationToken);
            if (entity == null)
            {
                return false;
            }

            await UpdateEntity(entity, EntityOperation.Delete, cancellationToken);
            return true;
        }

        protected async Task ApplyBatchOperations(EntityOperation operation, IEnumerable<T> entities, CancellationToken cancellationToken)
        {
            var batchedClient = CreateTableBatchClient();

            foreach (var entity in entities)
            {
                if (cancellationToken.IsCancellationRequested) break;

                switch (operation)
                {
                    case EntityOperation.Add:
                        batchedClient.Insert(_entityAdapter.ToEntityModel(entity));
                        break;

                    case EntityOperation.AddOrMerge:
                        batchedClient.InsertOrMerge(_entityAdapter.ToEntityModel(entity));
                        break;

                    case EntityOperation.AddOrReplace:
                        batchedClient.InsertOrReplace(_entityAdapter.ToEntityModel(entity));
                        break;

                    case EntityOperation.Delete:
                        batchedClient.Delete(_entityAdapter.ToEntityModel(entity));
                        break;
                }

                await batchedClient.SubmitToPipelineAsync(_entityKeyBuilder.ResolvePartitionKey(entity), cancellationToken);
            }
            await batchedClient.CommitTransactionAsync();
            await NotifyCompleteAsync();
        }
    }
}