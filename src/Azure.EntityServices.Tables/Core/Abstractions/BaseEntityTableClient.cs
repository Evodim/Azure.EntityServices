using Azure.EntityServices.Queries;
using Azure.EntityServices.Tables;
using Azure.EntityServices.Tables.Core;
using Azure.EntityServices.Tables.Core.Abstractions;
using Azure.EntityServices.Tables.Extensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Azure.EntityServices.Core.Abstractions
{
    /// <summary>
    /// Client to manage entities in azure Tables
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public abstract class BaseEntityTableClient<T> : IEntityTableClient<T>, IObservableEntityTableClient<T>
    where T : class, new()
    {
        private readonly IEntityAdapterFactory _entityAdapterFactory;
        private readonly ITableClientFactory _tableClientFactory;

        private IEntityAdapter<T> _entityAdapter;

        private ITableClientFacade<T> _tableClientFacade;        
        private IEnumerable<string> _indextedTags;
        private IList<string> _indextedTagsWithKeys;
        private IEnumerable<IEntityObserver<T>> _observerInstances;
        private IEntityObserverNotifier<T> _entityObserverNotifier;
        private Func<IEnumerable<EntityOperation>, Task> _submittedObserver;
        private Func<EntityTransactionGroup, Task<EntityTransactionGroup>> _pipelinePreProcessor;

        private EntityKeyBuilder<T> _entityKeyBuilder;
        protected EntityTableClientConfig<T> Config;

        protected EntityTableClientOptions Options;

        protected BaseEntityTableClient(IEntityAdapterFactory entityAdapterFactory, ITableClientFactory tableClientFactory)
        {
            _entityAdapterFactory = entityAdapterFactory;
            _tableClientFactory = tableClientFactory;
        }
        private async Task UpdateEntity(T entity, EntityOperationType operation, CancellationToken cancellationToken = default)
        {
            var batchClient = _tableClientFactory.Create(
                Config,
                Options,
                _pipelinePreProcessor,
                _entityAdapter,
                _submittedObserver);

            try
            {
                batchClient.AddOperation(operation, entity);
                await batchClient.SendOperation(cancellationToken);

                await _entityObserverNotifier.NotifyCompleteAsync();
            }
            catch (Exception ex)
            {
                await _entityObserverNotifier.NotifyExceptionAsync(ex);
                throw new EntityTableClientException($"An error occured during the request, partition:{_entityKeyBuilder.ResolvePartitionKey(entity)} rowkey:{_entityKeyBuilder.ResolvePrimaryKey(entity)}", ex);
            }
        }

        public virtual IEntityTableClient<T> Configure(EntityTableClientOptions options, EntityTableClientConfig<T> config)
        {
            _ = options ?? throw new ArgumentNullException(nameof(options));
            _ = config ?? throw new ArgumentNullException(nameof(config));

            Options = options;
            Config = config;
            Config.RowKeyResolver ??= (e) => Config.RowKeyProp.GetValue(e);
            Config.PartitionKeyResolver ??= (e) => $"_{Config.RowKeyResolver(e).ToInvariantString().ToShortHash()}";
            _entityKeyBuilder = new EntityKeyBuilder<T>(Config.PartitionKeyResolver, Config.RowKeyResolver);
            _observerInstances = Config.Observers.Select(o => o.Value.Invoke()).ToList();
            _entityObserverNotifier = new EntityObserverNotifier<T>(_observerInstances);

            //TableName required
            if (string.IsNullOrWhiteSpace(Options.TableName))
            {
                throw new ArgumentNullException(nameof(Options.TableName));
            }
            //PrimaryKey required
            _ = Config.RowKeyResolver ?? throw new InvalidOperationException($"at least one of RowKeyResolver or PrimaryKeyResolver was required and must be set");

            ConfigureServiceFactories(_entityAdapterFactory, _tableClientFactory);

            _submittedObserver = transactions => _entityObserverNotifier.NotifyChangeAsync(transactions.Select(
                transaction => new EntityOperationContext<T>(
                    transaction.PartitionKey,
                    transaction.RowKey,
                    new TableEntityDataReader<T>(transaction.NativeProperties, _entityAdapter),
                    transaction.EntityOperationType)));

            _indextedTags =
              Config.Tags.Keys
              .Union(Config.ComputedTags)
              .Select(t => _entityKeyBuilder.CreateTagName(t))
              .Where(t => t.EndsWith(_entityKeyBuilder.IndexedTagSuffix));

            _indextedTagsWithKeys = _indextedTags.ToList();
            _indextedTagsWithKeys.Add("PartitionKey");
            _indextedTagsWithKeys.Add("RowKey");

            _pipelinePreProcessor = async transaction =>
            {
                var newEntity = transaction.Actions.FirstOrDefault();

                var existingEntity = default(IDictionary<string, object>);
                var mainEntityAction = transaction.Actions.First();
                if (mainEntityAction.EntityOperationType != EntityOperationType.Add && Options.HandleTagMutation)
                {
                    existingEntity = await _tableClientFacade.GetEntityProperties(transaction.PartitionKey, newEntity.RowKey, _indextedTagsWithKeys);
                }
                //duplicate entity per tags in same partition
                foreach (var tag in _indextedTags)
                {
                    //handle tag mutation
                    if (existingEntity != null &&
                        existingEntity.TryGetValue(tag, out var existingtagValue) &&
                        newEntity.NativeProperties[tag] is string tagValue && tagValue != existingtagValue as string
                        )
                    {
                        //tag value was changed, existing tag entity must be deleted

                        var oldEntityTagAction = mainEntityAction with
                        {
                            EntityOperationType = EntityOperationType.Delete,
                            RowKey = existingtagValue as string
                        };

                        oldEntityTagAction.RowKey = existingtagValue as string;

                        transaction.Actions.Add(oldEntityTagAction);

                        //no needs to add new tag entity
                        if (mainEntityAction.EntityOperationType == EntityOperationType.Delete)
                        {
                            continue;
                        }
                    }

                    var newEntityTagAction = newEntity with
                    {
                        EntityOperationType = mainEntityAction.EntityOperationType == EntityOperationType.Delete ? EntityOperationType.Delete :
                        EntityOperationType.AddOrReplace,
                        RowKey = newEntity.NativeProperties[tag] as string
                    };
                    transaction.Actions.Add(newEntityTagAction);
                }
                return transaction;
            };

            return this;
        }

        public virtual void ConfigureServiceFactories(
            IEntityAdapterFactory entityAdapterFactory,
            ITableClientFactory tableClientFactory)
        {
            _ = entityAdapterFactory ?? throw new ArgumentNullException(nameof(entityAdapterFactory));
            _ = tableClientFactory ?? throw new ArgumentNullException(nameof(tableClientFactory));

            _entityAdapter = entityAdapterFactory.Create(_entityKeyBuilder, Config, Options);
            _tableClientFacade = tableClientFactory.Create(Config, Options, _pipelinePreProcessor, _entityAdapter, _submittedObserver);
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
                pageEnumerator = _tableClientFacade.QueryEntities(filter, maxPerPage, continuationToken, cancellationToken: cancellationToken)
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

        public async Task<T> GetByIdAsync(string partition, object id, CancellationToken cancellationToken = default)
        {
            var rowKey = _entityKeyBuilder.ResolvePrimaryKey(id);
            try
            {
                var response = await _tableClientFacade.GetEntityProperties(partition.EscapeDisallowedKeyValue(), rowKey, null, cancellationToken);
                if (response == null)
                {
                    return null;
                }
                return _entityAdapter.FromEntityModel(response);
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
            await foreach (var page in _tableClientFacade.QueryEntities(filter, null, null, cancellationToken: cancellationToken))
            {
                yield return page.Entities;
            }
        }

        public Task AddOrReplaceAsync(T entity, CancellationToken cancellationToken = default)
        {
            return UpdateEntity(entity, EntityOperationType.AddOrReplace, cancellationToken);
        }

        public Task AddOrMergeAsync(T entity, CancellationToken cancellationToken = default)
        {
            return UpdateEntity(entity, EntityOperationType.AddOrMerge, cancellationToken);
        }

        public Task AddAsync(T entity, CancellationToken cancellationToken = default)
        {
            return UpdateEntity(entity, EntityOperationType.Add, cancellationToken);
        }

        public Task ReplaceAsync(T entity, CancellationToken cancellationToken = default)
        {
            return UpdateEntity(entity, EntityOperationType.Replace, cancellationToken);
        }

        public Task MergeAsync(T entity, CancellationToken cancellationToken = default)
        {
            return UpdateEntity(entity, EntityOperationType.Merge, cancellationToken);
        }

        public Task AddManyAsync(IEnumerable<T> entities, CancellationToken cancellationToken = default)
        {
            return ApplyBatchOperations(EntityOperationType.Add, entities, cancellationToken);
        }

        public Task AddOrReplaceManyAsync(IEnumerable<T> entities, CancellationToken cancellationToken = default)
        {
            return ApplyBatchOperations(EntityOperationType.AddOrReplace, entities, cancellationToken);
        }

        public Task AddOrMergeManyAsync(IEnumerable<T> entities, CancellationToken cancellationToken = default)
        {
            return ApplyBatchOperations(EntityOperationType.AddOrMerge, entities, cancellationToken);
        }

        public async Task<long> UpdateManyAsync(Action<T> updateAction, Action<IQuery<T>> filter = default, CancellationToken cancellationToken = default)
        {
            long count = 0;
            var batchedClient = _tableClientFactory.Create(
                Config,
                Options,
                _pipelinePreProcessor,
                _entityAdapter,
                _submittedObserver);

            await foreach (var entityPage in _tableClientFacade.QueryEntities(filter, null, null, cancellationToken: cancellationToken))
            {
                await AddOrMergeManyAsync(entityPage.Entities.ToList()
                    .Select(e =>
                    {
                        updateAction.Invoke(e);
                        return e;
                    }), cancellationToken);

                count += entityPage.IteratedCount;
#if DEBUG
                System.Diagnostics.Debug.WriteLine("Entities updated {0}", count);
#endif
            }

            return count;
        }

        public void AddObserver(string name, Func<IEntityObserver<T>> observerFactory)
        {
            Config.Observers.TryAdd(name, observerFactory);
        }

        public void RemoveObserver(string name)
        {
            Config.Observers.TryRemove(name, out var _);
        }

        public Task DropTableAsync(CancellationToken cancellationToken = default)
        {
            return _tableClientFacade.DropTableIfExists(cancellationToken);
        }

        public Task CreateTableAsync(CancellationToken cancellationToken = default)
        {
            return _tableClientFacade.CreateTableIfNotExists(cancellationToken);
        }

        public Task DeleteManyAsync(IEnumerable<T> entities, CancellationToken cancellationToken = default)
        {
            return ApplyBatchOperations(EntityOperationType.Delete, entities, cancellationToken);
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

            await UpdateEntity(entity, EntityOperationType.Delete, cancellationToken);
            return true;
        }

        protected async Task ApplyBatchOperations(EntityOperationType operationType, IEnumerable<T> entities, CancellationToken cancellationToken)
        {
            var batchedClient = _tableClientFactory.Create
                (Config,
                Options,
                _pipelinePreProcessor,
                _entityAdapter,
                _submittedObserver);

            foreach (var entity in entities)
            {
                if (cancellationToken.IsCancellationRequested) break;
                batchedClient.AddOperation(operationType, entity);

                await batchedClient.SendOperations(_entityKeyBuilder.ResolvePartitionKey(entity), cancellationToken);
            }
            await batchedClient.CompletePipelineAsync();
            await _entityObserverNotifier.NotifyCompleteAsync();
        }
    }
}