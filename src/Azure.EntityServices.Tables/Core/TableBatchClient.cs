using Azure.Data.Tables;
using Polly.Retry;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Azure.EntityServices.Tables.Core
{
    public class TableBatchClient
    {
        private readonly AsyncRetryPolicy _retryPolicy;
        private readonly TableBatchClientOptions _options;
        private readonly Func<IEnumerable<TableTransactionAction>, Task> _observer;
        private readonly Func<EntityTransactionGroup, Task<EntityTransactionGroup>> _preProcessor;
        private readonly TableServiceClient _tableClientService;
        private readonly Queue<TableTransactionAction> _pendingOperations; 
        private IEntityTransactionGroupPipeline _pipeline;
#if DEBUG
        private int _taskCount = 0;
#endif

        public TableBatchClient(
            TableServiceClient tableClientService,
            TableBatchClientOptions options,
            AsyncRetryPolicy retryPolicy,
            Func<EntityTransactionGroup, Task<EntityTransactionGroup>> preProcessor,
            Func<IEnumerable<TableTransactionAction>, Task> observer)
        {
            _ = options ?? throw new ArgumentNullException(nameof(options));
            _ = retryPolicy ?? throw new ArgumentNullException(nameof(retryPolicy));

            _pendingOperations = new Queue<TableTransactionAction>(); 
            _retryPolicy = retryPolicy;
            _options = options;
            _observer = observer;
            _preProcessor = preProcessor;
            _tableClientService = tableClientService;
        }

        public decimal OutstandingOperations => _pendingOperations.Count;

        public void Insert<TEntity>(TEntity entity)
            where TEntity : ITableEntity
        {
            _pendingOperations.Enqueue(new TableTransactionAction(TableTransactionActionType.Add, entity));
        }

        public void Delete<TEntity>(TEntity entity)
            where TEntity : ITableEntity
        {
            _pendingOperations.Enqueue(new TableTransactionAction(TableTransactionActionType.Delete, entity));
        }

        public void InsertOrMerge<TEntity>(TEntity entity)
            where TEntity : ITableEntity
        {
            _pendingOperations.Enqueue(new TableTransactionAction(TableTransactionActionType.UpsertMerge, entity));
        }

        public void InsertOrReplace<TEntity>(TEntity entity)
            where TEntity : ITableEntity
        {
            _pendingOperations.Enqueue(new TableTransactionAction(TableTransactionActionType.UpsertReplace, entity));
        }

        public void Merge<TEntity>(TEntity entity)
            where TEntity : ITableEntity
        {
            _pendingOperations.Enqueue(new TableTransactionAction(TableTransactionActionType.UpdateMerge, entity));
        }

        public void Replace<TEntity>(TEntity entity)
            where TEntity : ITableEntity
        {
            _pendingOperations.Enqueue(new TableTransactionAction(TableTransactionActionType.UpdateReplace, entity));
        }

        public async Task SubmitToPipelineAsync(string partitionKey, CancellationToken cancellationToken = default)
        {
            var client = _tableClientService.GetTableClient(_options.TableName);
            if (_pipeline == null)
            {
                _pipeline = CustomTplBlocks.CreatePipeline(_preProcessor, async transactions =>
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

                           await _retryPolicy.ExecuteAsync(() => client.SubmitTransactionAsync(operations, cancellationToken));

                           if (_observer != null)
                           {
                               await _observer.Invoke(operations);
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
            }

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

                var actions = new EntityTransactionGroup(_pendingOperations.First().Entity.PartitionKey);
                actions.Actions.Add(_pendingOperations.Dequeue());
                var actionsWithTags = await _preProcessor(actions);
                await _retryPolicy.ExecuteAsync(() => client.SubmitTransactionAsync(actionsWithTags.Actions, cancellationToken));

                if (_observer != null)
                {
                    await _observer.Invoke(actionsWithTags.Actions);
                }
                _pendingOperations.Clear();
            }
        }
    }
}