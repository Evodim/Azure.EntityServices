using Azure.EntityServices.Queries;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Azure.EntityServices.Tables.Core.Abstractions
{
    public class TableClientFacade<T> : ITableClientFacade<T> where T : class, new()
    {
        private readonly TableBatchClientOptions _options;
        private readonly Func<EntityTransactionGroup, Task<EntityTransactionGroup>> _preProcessor; 
        private readonly Queue<EntityOperation> _pendingOperations;
        private readonly IEntityTransactionGroupPipeline _pipeline;
        private readonly Func<IEnumerable<EntityOperation>, Task> _onSubmitted;
        private readonly IEntityAdapter<T> _entityAdapter;
        private readonly INativeTableClient<T> _nativeTableClient;

#if DEBUG
        private int _taskCount = 0;
#endif

        public TableClientFacade(
            INativeTableClient<T> tableClient,
            TableBatchClientOptions options,
            Func<EntityTransactionGroup, Task<EntityTransactionGroup>> preProcessor,
            IEntityAdapter<T> entityAdapter,
            Func<IEnumerable<EntityOperation>, Task> onTransactionSubmittedHandler = null
            )
        {
            _ = options ?? throw new ArgumentNullException(nameof(options));

            _nativeTableClient = tableClient;
            _pendingOperations = new Queue<EntityOperation>();
            _options = options;
            _preProcessor = preProcessor;
            _onSubmitted = onTransactionSubmittedHandler;
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

                    await _nativeTableClient.SubmitTransaction(operations);

                    if (_onSubmitted != null)
                    {
                        await _onSubmitted.Invoke(operations);
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

        public async Task SendOperations(string partitionKey, CancellationToken cancellationToken = default)
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

        public async Task SendOperation(CancellationToken cancellationToken = default)
        {
            if (_pendingOperations.Count != 0)
            {
                var actions = new EntityTransactionGroup(_pendingOperations.First().PartitionKey);
                actions.Actions.Add(_pendingOperations.Dequeue());
                var actionsWithTags = await _preProcessor(actions);

                await _nativeTableClient.SubmitTransaction(actionsWithTags.Actions);
                _pendingOperations.Clear();

                if (_onSubmitted != null)
                {
                    await _onSubmitted.Invoke(actionsWithTags.Actions);
                }
            }
        }

        public Task<bool> CreateTableIfNotExists(CancellationToken cancellationToken = default)
        {
            return _nativeTableClient.CreateTableIfNotExists(cancellationToken);
        }

        public Task<bool> DropTableIfExists(CancellationToken cancellationToken = default)
        {
           return _nativeTableClient.DropTableIfExists(cancellationToken);
        }

        public Task<IDictionary<string, object>> GetEntityProperties(string partitionKey, string rowKey, IEnumerable<string> properties = null, CancellationToken cancellationToken = default)
        {
           return _nativeTableClient.GetEntityProperties(partitionKey, rowKey, properties, cancellationToken);
        }

        public IAsyncEnumerable<EntityPage<T>> QueryEntities(Action<IQuery<T>> filter, int? maxPerPage, string nextPageToken, bool? iterateOnly = false, CancellationToken cancellationToken = default)
        {
          return _nativeTableClient.QueryEntities( filter, maxPerPage, nextPageToken, iterateOnly, cancellationToken);
        }
    }
}