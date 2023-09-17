using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Azure.EntityServices.Tables.Core.Abstractions
{
    public abstract class BaseTableBatchClient<T> : ITableBatchClient<T> where T : class, new()
    {
        private readonly TableBatchClientOptions _options;
        private readonly Func<EntityTransactionGroup, Task<EntityTransactionGroup>> _preProcessor;
        private readonly Queue<EntityOperation> _pendingOperations;
        private IEntityTransactionGroupPipeline _pipeline;
        private readonly Func<IEnumerable<EntityOperation>, Task> _onTransactionSubmitted;
        private readonly IEntityAdapter<T> _entityAdapter;

#if DEBUG
        private int _taskCount = 0;
#endif

        public BaseTableBatchClient(
            TableBatchClientOptions options,
            Func<EntityTransactionGroup, Task<EntityTransactionGroup>> preProcessor,
            IEntityAdapter<T> entityAdapter,
            Func<IEnumerable<EntityOperation>, Task> onTransactionSubmittedHandler = null
            )
        {
            _ = options ?? throw new ArgumentNullException(nameof(options));

            _pendingOperations = new Queue<EntityOperation>();

            _options = options;
            _preProcessor = preProcessor;
            _onTransactionSubmitted = onTransactionSubmittedHandler;
            _entityAdapter = entityAdapter;

            _pipeline ??= CustomTplBlocks.CreatePipeline(_preProcessor, async transactionGroups =>
            {
#if DEBUG

                try
                {
                    System.Diagnostics.Debug.WriteLine("pipeline task count {0}/{1}",
                    Interlocked.Increment(ref _taskCount), _options.MaxParallelTasks);
#endif
                    var operations = transactionGroups.SelectMany(t => t.Actions).ToList();
                    if (!operations.Any())
                    {
                        return;
                    }

#if DEBUG
                    System.Diagnostics.Debug.WriteLine("Operations to submit to the pipeline: {0}", operations.Count);
#endif

                    await SendBulkOperations(operations);

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
        }

        public decimal OutstandingOperations => _pendingOperations.Count;

        public void AddOperation(EntityOperationType entityOperationType, T entity)
        {
            _pendingOperations.Enqueue(
             _entityAdapter.ToEntityOperationAction(entityOperationType, entity)
             );
        }

        public async Task SendToPipelineAsync(string partitionKey, CancellationToken cancellationToken = default)
        {
            var entityTransactionGroup = new EntityTransactionGroup(partitionKey);
            entityTransactionGroup.Actions.AddRange(_pendingOperations);
            _pendingOperations.Clear();
            await _pipeline.SendAsync(entityTransactionGroup, cancellationToken);
        }

        public Task CompletePipelineAsync()
        {
            return _pipeline == null ? Task.CompletedTask : _pipeline.CompleteAsync();
        }

        public async Task SubmitAsync(CancellationToken cancellationToken = default)
        {
            if (_pendingOperations.Count != 0)
            {
                var actions = new EntityTransactionGroup(_pendingOperations.First().PartitionKey);
                actions.Actions.Add(_pendingOperations.Dequeue());
                var actionsWithTags = await _preProcessor(actions);

                await SendBulkOperations(actionsWithTags.Actions);
                _pendingOperations.Clear();

                if (_onTransactionSubmitted != null)
                {
                    await _onTransactionSubmitted.Invoke(actionsWithTags.Actions);
                }
            }
        }

        protected abstract Task SendBulkOperations(IEnumerable<EntityOperation> entityOperations);
    }
}