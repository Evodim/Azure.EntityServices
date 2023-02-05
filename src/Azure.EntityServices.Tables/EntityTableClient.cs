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
    public class EntityTableClient<T> : IEntityTableClient<T>
    where T : class, new()
    {
        private EntityTableClientConfig<T> _config;

        private EntityTableClientOptions _options;

        private RetryPolicy _retryPolicy;

        private AsyncRetryPolicy _asyncRetryPolicy;

        private TableClient _client;

        private TableClient _configuredClient => _client ?? throw new InvalidOperationException("EntityTableClient was not configured");

        private readonly TableServiceClient _tableServiceClient;

        private Func<IEnumerable<TableTransactionAction>, Task> _pipelineObserver;

        private IEnumerable<string> _indextedTags;

        private IEnumerable<IEntityObserver<T>> _observerInstances;

        private Func<EntityTransactionGroup, Task<EntityTransactionGroup>> _pipelinePreProcessor;

        private EntityTagBuilder<T> _entityTagBuilder;

        private IEntityBinder<T> CreatePrimaryEntityBinderFromEntity(T entity)
          => new TableEntityBinder<T>(entity, ResolvePartitionKey(entity), ResolvePrimaryKey(entity), _config.IgnoredProps, _entityTagBuilder);

        private IEntityBinder<T> CreateEntityBinderFromTableEntity(TableEntity tableEntity)
            => new TableEntityBinder<T>(tableEntity, _config.IgnoredProps, _entityTagBuilder);

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

        private IAsyncEnumerable<Page<TableEntity>> QueryEntities(Action<IQuery<T>> filter, int? maxPerPage, string nextPageToken, CancellationToken cancellationToken)
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
            .QueryAsync<TableEntity>(filter:string.IsNullOrWhiteSpace(strQuery)?null: strQuery,
                                     cancellationToken: cancellationToken,
                                     maxPerPage: maxPerPage)
            .AsPages(nextPageToken));
        }

        private async Task UpdateEntity(T entity, EntityOperation operation, CancellationToken cancellationToken = default)
        {
            var client = CreateTableBatchClient();

            var entityBinder = CreatePrimaryEntityBinderFromEntity(entity);

            try
            {
                entityBinder.BindDynamicProps(_config.DynamicProps);
                entityBinder.BindTags(_config.Tags, _config.ComputedTags);

                var tableEntity = entityBinder.Bind();
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
                throw new EntityTableClientException($"An error occured during the request, partition:{entityBinder?.PartitionKey} rowkey:{entityBinder?.RowKey}", ex);
            }
        }

        protected async Task NotifyChangeAsync(IEnumerable<IEntityBinderContext<T>> context)
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
            _config.PartitionKeyResolver ??= (e) => $"_{ResolvePrimaryKey(e).ToShortHash()}";
            _client ??= _tableServiceClient.GetTableClient(options.TableName);
            _observerInstances = _config.Observers.Select(o => o.Value.Invoke()).ToList();

            _entityTagBuilder = new EntityTagBuilder<T>(ResolvePrimaryKey);
            //PrimaryKey required
            if (_config.RowKeyResolver == null && _config.RowKeyProp == null)
            {
                throw new InvalidOperationException($"One of PrimaryKeyProp or PrimaryKeyResolver was required and must be set");
            }

            var basePolicy = Policy.Handle<RequestFailedException>(ex => HandleStorageException(options.TableName, _tableServiceClient, options.CreateTableIfNotExists, ex));

            _retryPolicy = basePolicy
                                .WaitAndRetry(5, i => TimeSpan.FromSeconds(2 * i));
            _asyncRetryPolicy = basePolicy
                                .WaitAndRetryAsync(5, i => TimeSpan.FromSeconds(2 * i));
            _config = config;

            _pipelineObserver = transactions => NotifyChangeAsync(transactions.Select(
                transaction => new EntityBinderContext<T>(CreateEntityBinderFromTableEntity(transaction.Entity as TableEntity),
                transaction.ActionType.MapToEntityOperation())));

            _indextedTags =
              _config.Tags.Keys
              .Union(_config.ComputedTags)
              .Select(t => _entityTagBuilder.CreateTagName(t))
              .Where(t => t.EndsWith(_entityTagBuilder.IndexedTagSuffix));

            _pipelinePreProcessor = async transaction =>
            {
                var newEntity = transaction.Actions.FirstOrDefault()?.Entity as TableEntity;

                var props = _indextedTags.ToList();

                props.AddRange(new List<string>() { "PartitionKey", "RowKey" });
                var existingEntity = default(NullableResponse<TableEntity>);
                var entityAction = transaction.Actions.First().ActionType;
                if (entityAction != TableTransactionActionType.Add && _options.HandleTagMutation)
                {
                    existingEntity = await _configuredClient.GetEntityIfExistsAsync<TableEntity>(newEntity.PartitionKey, newEntity.RowKey, props);
                }
                //project entity tags in same partition group
                foreach (var tag in _indextedTags)
                {
                    if (existingEntity != null &&
                        existingEntity.HasValue &&
                        existingEntity.Value.TryGetValue(tag, out var existingtagValue) &&
                        newEntity[tag] is string tagValue && tagValue != existingtagValue as string
                        )
                    {
                        var oldEntityTag = new TableEntity(existingEntity.Value);
                        var oldEntityTagAction = new TableTransactionAction(TableTransactionActionType.Delete, oldEntityTag);
                        oldEntityTagAction.Entity.RowKey = existingtagValue as string;
                        transaction.Actions.Add(oldEntityTagAction);
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
            return this;
        }

        public async Task<T> GetByIdAsync(string partition, object id, CancellationToken cancellationToken = default)
        {
            var rowKey = ResolvePrimaryKey(id);
            try
            {
                var response = await _asyncRetryPolicy.ExecuteAsync(async () => await _configuredClient.GetEntityIfExistsAsync<TableEntity>(partition.EscapeDisallowedChars(), rowKey, select: new string[] { }, cancellationToken));
                if (!response.HasValue)
                {
                    return null;
                }
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

        public async IAsyncEnumerable<IEnumerable<T>> GetAsync(Action<IQuery<T>> filter = default,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await foreach (var page in QueryEntities(filter, null, null, cancellationToken))
            {
                yield return page.Values.Select(tableEntity => CreateEntityBinderFromTableEntity(tableEntity).UnBind());
            }
        }

        public async Task<EntityPage<T>> GetPagedAsync(
            Action<IQuery<T>> filter = default,
            int? maxPerPage = null,
            string nextPageToken = null,
            CancellationToken cancellationToken = default
            )
        {
            var pageEnumerator =
                QueryEntities(filter, maxPerPage, nextPageToken, cancellationToken)
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

        private async Task ApplyBatchOperations(EntityOperation operation, IEnumerable<T> entities, CancellationToken cancellationToken)
        {
            var batchedClient = CreateTableBatchClient();

            foreach (var entity in entities)
            {
                if (cancellationToken.IsCancellationRequested) break;

                var entityBinder = CreatePrimaryEntityBinderFromEntity(entity);

                entityBinder.BindDynamicProps(_config.DynamicProps);
                entityBinder.BindTags(_config.Tags, _config.ComputedTags);

                switch (operation)
                {
                    case EntityOperation.Add:
                        batchedClient.Insert(entityBinder.Bind());
                        break;

                    case EntityOperation.AddOrMerge:
                        batchedClient.InsertOrMerge(entityBinder.Bind());
                        break;

                    case EntityOperation.AddOrReplace:
                        batchedClient.InsertOrReplace(entityBinder.Bind());
                        break;

                    case EntityOperation.Delete:
                        batchedClient.Delete(entityBinder.Bind());
                        break;
                }

                await batchedClient.SubmitToPipelineAsync(entityBinder.PartitionKey, cancellationToken);
            }
            await batchedClient.CommitTransactionAsync();
            await NotifyCompleteAsync();
        }

        public async Task<long> UpdateManyAsync(Action<T> updateAction, Action<IQuery<T>> filter = default, CancellationToken cancellationToken = default)
        {
            long count = 0;
            var batchedClient = CreateTableBatchClient();

            await foreach (var page in QueryEntities(filter, null, null, cancellationToken))
            {
                foreach (var tableEntity in page.Values)
                {
                    if (cancellationToken.IsCancellationRequested) break;

                    var binder = CreateEntityBinderFromTableEntity(tableEntity);
                    var entity = binder.UnBind();

                    updateAction.Invoke(entity);

                    binder.BindDynamicProps(_config.DynamicProps);
                    binder.BindTags(_config.Tags, _config.ComputedTags);

                    batchedClient.InsertOrMerge(binder.Bind());

                    await batchedClient.SubmitToPipelineAsync(binder.PartitionKey, cancellationToken);
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

        public Task DeleteAsync(T entity, CancellationToken cancellationToken = default)
        {
            return UpdateEntity(entity, EntityOperation.Delete, cancellationToken);
        }

        public async Task<IDictionary<string, object>> GetEntityMetadatasAsync(string partitionKey, string rowKey, CancellationToken cancellationToken = default)
        {
            var metadataKeys = _config.Tags.Keys.Union(_config.ComputedTags).Select(k => _entityTagBuilder.CreateTagName(k));

            try
            {
                var response = await _asyncRetryPolicy.ExecuteAsync(async () => await _configuredClient.GetEntityAsync<TableEntity>(partitionKey, rowKey, metadataKeys, cancellationToken));
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

        public string ResolvePartitionKey(T entity) => TableQueryHelper.ToPartitionKey(_config.PartitionKeyResolver(entity));

        public string ResolvePrimaryKey(T entity)
        {
            return TableQueryHelper.ToPrimaryRowKey(_config.RowKeyResolver?.Invoke(entity) ?? _config.RowKeyProp.GetValue(entity));
        }

        public string ResolvePrimaryKey(object value)
        {
            return TableQueryHelper.ToPrimaryRowKey(value);
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
    }
}