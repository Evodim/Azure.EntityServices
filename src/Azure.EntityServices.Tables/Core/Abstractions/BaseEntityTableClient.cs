using Azure.EntityServices.Queries;
using Azure.EntityServices.Tables;
using Azure.EntityServices.Tables.Core;
using Azure.EntityServices.Tables.Core.Abstractions;
using Azure.EntityServices.Tables.Core.Implementations;
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
        private INativeTableClient<T> _nativeTableClient;
        private INativeTableBatchClientFactory<T> _nativeTableBatchClientFactory;
        private IEntityAdapter<T> _entityAdapter;
        private IEnumerable<string> _indextedTags;
        private IList<string> _indextedTagsWithKeys;
        private IEnumerable<IEntityObserver<T>> _observerInstances;
        private IEntityObserverNotifier<T> _entityObserverNotifier;
        private OnTransactionSubmitted _submittedObserver;
        private Func<EntityTransactionGroup, Task<EntityTransactionGroup>> _pipelinePreProcessor;

        protected EntityKeyBuilder<T> EntityKeyBuilder;

        protected EntityTableClientConfig<T> Config;

        protected EntityTableClientOptions Options;

        private async Task UpdateEntity(T entity, EntityOperationType operation, CancellationToken cancellationToken = default)
        {
            var batchClient = _nativeTableBatchClientFactory.Create(Options, _pipelinePreProcessor, _entityAdapter, _submittedObserver);

            try
            {
                switch (operation)
                {
                    case EntityOperationType.Add:
                        batchClient.Insert(entity);
                        break;

                    case EntityOperationType.AddOrMerge:
                        batchClient.InsertOrMerge(entity);
                        break;

                    case EntityOperationType.AddOrReplace:
                        batchClient.InsertOrReplace(entity);
                        break;

                    case EntityOperationType.Replace:
                        batchClient.Replace(entity);
                        break;

                    case EntityOperationType.Merge:
                        batchClient.Merge(entity);
                        break;

                    case EntityOperationType.Delete:
                        batchClient.Delete(entity);
                        break;

                    default:
                        throw new NotImplementedException($"{operation} operation not supported");
                }
                await batchClient.SubmitToStorageAsync(cancellationToken);
                await _entityObserverNotifier.NotifyCompleteAsync();
            }
            catch (Exception ex)
            {
                await _entityObserverNotifier.NotifyExceptionAsync(ex);
                throw new EntityTableClientException($"An error occured during the request, partition:{EntityKeyBuilder.ResolvePartitionKey(entity)} rowkey:{EntityKeyBuilder.ResolvePrimaryKey(entity)}", ex);
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
            EntityKeyBuilder = new EntityKeyBuilder<T>(Config.PartitionKeyResolver, Config.RowKeyResolver);
            _observerInstances = Config.Observers.Select(o => o.Value.Invoke()).ToList();
            _entityObserverNotifier = new EntityObserverNotifier<T>(_observerInstances);

            //TableName required
            if (string.IsNullOrWhiteSpace(Options.TableName))
            {
                throw new ArgumentNullException(nameof(Options.TableName));
            }
            //PrimaryKey required
            _ = Config.RowKeyResolver ?? throw new InvalidOperationException($"at least one of RowKeyResolver or PrimaryKeyResolver was required and must be set");


            ConfigureServices(_nativeTableClient, _entityAdapter, _nativeTableBatchClientFactory);

            _submittedObserver = transactions => _entityObserverNotifier.NotifyChangeAsync(transactions.Select(
                transaction => new EntityOperationContext<T>(
                    transaction.PartitionKey,
                    transaction.RowKey,
                    new TableEntityDataReader<T>(transaction.NativeProperties, _entityAdapter),
                    transaction.EntityOperationType)));

            _indextedTags =
              Config.Tags.Keys
              .Union(Config.ComputedTags)
              .Select(t => EntityKeyBuilder.CreateTagName(t))
              .Where(t => t.EndsWith(EntityKeyBuilder.IndexedTagSuffix));

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
                    existingEntity = await _nativeTableClient.GetEntityProperties(transaction.PartitionKey, newEntity.RowKey, _indextedTagsWithKeys);
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
      
        public virtual void ConfigureServices(
            INativeTableClient<T> nativeTableClient,
            IEntityAdapter<T> entityAdapter,
            INativeTableBatchClientFactory<T> nativeTableBatchClientFactory)
        {
            _ = nativeTableClient ?? throw new ArgumentNullException(nameof(nativeTableClient));
            _ = entityAdapter ?? throw new ArgumentNullException(nameof(entityAdapter));
            _ = nativeTableBatchClientFactory ?? throw new ArgumentNullException(nameof(nativeTableBatchClientFactory));

            _nativeTableClient = nativeTableClient;
            _entityAdapter = entityAdapter;
            _nativeTableBatchClientFactory = nativeTableBatchClientFactory;
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
                pageEnumerator = _nativeTableClient.QueryEntities(filter, maxPerPage, continuationToken, cancellationToken)
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
            var rowKey = EntityKeyBuilder.ResolvePrimaryKey(id);
            try
            {
                var response = await _nativeTableClient.GetEntityProperties(partition.EscapeDisallowedKeyValue(), rowKey, null, cancellationToken);
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
            await foreach (var page in _nativeTableClient.QueryEntities(filter, null, null, cancellationToken))
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
            var batchedClient = _nativeTableBatchClientFactory.Create(Options, _pipelinePreProcessor, _entityAdapter, _submittedObserver);

            await foreach (var entityPage in _nativeTableClient.QueryEntities(filter, null, null, cancellationToken))
            {
                foreach (var entity in entityPage.Entities)
                {
                    if (cancellationToken.IsCancellationRequested) break;

                    updateAction.Invoke(entity);

                    batchedClient.InsertOrMerge(entity);

                    await batchedClient.SubmitToPipelineAsync(EntityKeyBuilder.ResolvePartitionKey(entity), cancellationToken);
                    count++;
                }
#if DEBUG
                System.Diagnostics.Debug.WriteLine("Entities updated {0}", count);
#endif
            }
            await batchedClient.CommitTransactionAsync();
            await _entityObserverNotifier.NotifyCompleteAsync();

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
            return _nativeTableClient.DropTableIfExists(cancellationToken);
        }

        public Task CreateTableAsync(CancellationToken cancellationToken = default)
        {
            return _nativeTableClient.CreateTableIfNotExists(cancellationToken);
        }

        public Task DeleteManyAsync(IEnumerable<T> entities, CancellationToken cancellationToken = default)
        {
            return ApplyBatchOperations(EntityOperationType.Delete, entities, cancellationToken);
        }

        public Task<bool> DeleteAsync(T entity, CancellationToken cancellationToken = default)
        {
            var partitionKey = EntityKeyBuilder.ResolvePartitionKey(entity);
            var rowKey = EntityKeyBuilder.ResolvePrimaryKey(entity);

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

        protected async Task ApplyBatchOperations(EntityOperationType operation, IEnumerable<T> entities, CancellationToken cancellationToken)
        {
            var batchedClient = _nativeTableBatchClientFactory.Create(Options, _pipelinePreProcessor, _entityAdapter, _submittedObserver);

            foreach (var entity in entities)
            {
                if (cancellationToken.IsCancellationRequested) break;

                switch (operation)
                {
                    case EntityOperationType.Add:
                        batchedClient.Insert(entity);
                        break;

                    case EntityOperationType.AddOrMerge:
                        batchedClient.InsertOrMerge(entity);
                        break;

                    case EntityOperationType.AddOrReplace:
                        batchedClient.InsertOrReplace(entity);
                        break;

                    case EntityOperationType.Delete:
                        batchedClient.Delete(entity);
                        break;
                }

                await batchedClient.SubmitToPipelineAsync(EntityKeyBuilder.ResolvePartitionKey(entity), cancellationToken);
            }
            await batchedClient.CommitTransactionAsync();
            await _entityObserverNotifier.NotifyCompleteAsync();
        }
    }
}