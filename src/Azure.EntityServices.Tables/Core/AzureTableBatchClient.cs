using Azure.Data.Tables;
using Azure.EntityServices.Tables.Extensions;
using Polly;
using Polly.Retry;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Azure.EntityServices.Tables.Core
{
    public delegate Task OnTransactionSubmitted(IEnumerable<EntityOperation> transaction);

    public class AzureTableBatchClient<T>
        where T : class, new()
    {
        private readonly AsyncRetryPolicy _retryPolicy;
        private readonly TableBatchClientOptions _options; 
        private readonly Func<EntityTransactionGroup, Task<EntityTransactionGroup>> _preProcessor;
        private readonly TableServiceClient _tableClientService;
        private readonly Queue<EntityOperation> _pendingOperations;
        private IEntityTransactionGroupPipeline _pipeline;
        private readonly OnTransactionSubmitted _onTransactionSubmitted;
        private readonly IEntityAdapter<T> _entityAdapter;


#if DEBUG
        private int _taskCount = 0;
#endif

        public AzureTableBatchClient(
            TableServiceClient tableServiceClient,
            TableBatchClientOptions options,
            Func<EntityTransactionGroup, Task<EntityTransactionGroup>> preProcessor,
            IEntityAdapter<T> entityAdapter,
            OnTransactionSubmitted onTransactionSubmittedHandler = null      
            )
        {
            _ = options ?? throw new ArgumentNullException(nameof(options));

            _pendingOperations = new Queue<EntityOperation>();

            _retryPolicy = Policy.Handle<RequestFailedException>(ex => ex.HandleAzureStorageException(options.TableName, tableServiceClient, options.CreateTableIfNotExists))
                .WaitAndRetryAsync(5, i => TimeSpan.FromSeconds(2 * i));

            _options = options;
            _preProcessor = preProcessor;
            _tableClientService = tableServiceClient;
            _onTransactionSubmitted = onTransactionSubmittedHandler;
            _entityAdapter = entityAdapter;
        }

        public decimal OutstandingOperations => _pendingOperations.Count;

        public void Insert(T entity)
          
        {
            _pendingOperations.Enqueue(
               _entityAdapter.ToEntityOperationAction(EntityOperationType.Add, entity)
               );
        }

        public void Delete(T entity)
           
        {
            _pendingOperations.Enqueue(
            _entityAdapter.ToEntityOperationAction(EntityOperationType.Delete, entity)
            );
        }

        public void InsertOrMerge(T entity)
           
        {
            _pendingOperations.Enqueue(
             _entityAdapter.ToEntityOperationAction(EntityOperationType.AddOrMerge, entity)
             );
        }

        public void InsertOrReplace(T entity)
            
        {
            _pendingOperations.Enqueue(
             _entityAdapter.ToEntityOperationAction(EntityOperationType.AddOrReplace, entity)
             );
        }

        public void Merge(T entity)
          
        {
            _pendingOperations.Enqueue(
               _entityAdapter.ToEntityOperationAction(EntityOperationType.Merge, entity)
               );
        }

        public void Replace(T entity)
            
        {
            _pendingOperations.Enqueue(
              _entityAdapter.ToEntityOperationAction(EntityOperationType.Replace, entity)
              );
        }

        public async Task SubmitToPipelineAsync(string partitionKey, CancellationToken cancellationToken = default)
        {
            var client = _tableClientService.GetTableClient(_options.TableName);
            _pipeline ??= CustomTplBlocks.CreatePipeline(_preProcessor, async transactions =>
                   {
#if DEBUG
                       try
                       {
                           System.Diagnostics.Debug.WriteLine("pipeline task count {0}/{1}",
                           Interlocked.Increment(ref _taskCount), _options.MaxParallelTasks);
#endif
                           var operations = transactions.SelectMany(t => t.Actions).ToList();
                           if (!operations.Any())
                           {
                               return;
                           }
#if DEBUG
                           System.Diagnostics.Debug.WriteLine("Operations to submit to the pipeline: {0}", operations.Count);
#endif

                           await _retryPolicy.ExecuteAsync(() => client.SubmitTransactionAsync(operations.Select(a =>

                              new TableTransactionAction(
                                  a.EntityOperationType.MapToTableTransactionActionType(),_entityAdapter.ToEntityModel<TableEntity>(new EntityModel(a.PartitionKey,a.RowKey,a.NativeProperties)))), cancellationToken));

                          
                           if (_onTransactionSubmitted != null)
                           {
                               await _onTransactionSubmitted.Invoke(operations);
                           }
#if DEBUG
                       }
                       finally
                       {
                           Interlocked.Decrement(ref _taskCount);
                       }
#endif
                   },
                maxItemInBatch: _options.MaxItemInBatch,
                maxItemInTransaction: _options.MaxItemInTransaction,
                maxParallelTasks: _options.MaxParallelTasks
                );

            var entityTransactionGroup = new EntityTransactionGroup(partitionKey);
            entityTransactionGroup.Actions.AddRange(_pendingOperations);
            _pendingOperations.Clear();
            await _pipeline.SendAsync(entityTransactionGroup, cancellationToken);
        }

        public Task CommitTransactionAsync()
        {
            return (_pipeline == null) ? Task.CompletedTask : _pipeline.CompleteAsync();
        }

        public async Task SubmitToStorageAsync(CancellationToken cancellationToken = default)
        {
            if (_pendingOperations.Count != 0)
            {
                var client = _tableClientService.GetTableClient(_options.TableName);

                var actions = new EntityTransactionGroup(_pendingOperations.First().PartitionKey);
                actions.Actions.Add(_pendingOperations.Dequeue());
                var actionsWithTags = await _preProcessor(actions);
                await _retryPolicy.ExecuteAsync(() => client.SubmitTransactionAsync(actionsWithTags.Actions.Select(a =>
                {
                    return new TableTransactionAction(a.EntityOperationType.MapToTableTransactionActionType(),
                        _entityAdapter.ToEntityModel<TableEntity>(new EntityModel(a.PartitionKey, a.RowKey, a.NativeProperties))
                        
                        );
                })
                , cancellationToken));

                if (_onTransactionSubmitted != null)
                {
                    await _onTransactionSubmitted.Invoke(actionsWithTags.Actions);
                }
                _pendingOperations.Clear();
            }
        }
    }
}