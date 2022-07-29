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
        private readonly Queue<TableTransactionAction> _pendingOperations;
        private readonly string _tableName;
        private readonly string _connectionString;
        private IPipeline _pipeline;
#if DEBUG
        private static int _taskCount = 0;
#endif

        public TableBatchClient(
            TableBatchClientOptions options,
            AsyncRetryPolicy retryPolicy)
        {
            _ = options ?? throw new ArgumentNullException(nameof(options));
            _ = retryPolicy ?? throw new ArgumentNullException(nameof(retryPolicy));

            _pendingOperations = new Queue<TableTransactionAction>();
            _connectionString = options.ConnectionString;
            _tableName = options.TableName;
            _retryPolicy = retryPolicy;
            _options = options;
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

        public Task SubmitToPipelineAsync(string partitionKey, CancellationToken cancellationToken = default)
        {
            if (_pipeline == null)
            {
                _pipeline = CustomTplBlocks.CreatePipeline(transactions =>
                   {
#if DEBUG
                       try
                       {
                           System.Diagnostics.Debug.WriteLine("pipeline task count {0}/{1}",
                       Interlocked.Increment(ref _taskCount), _options.MaxParallelTasks);
#endif
                           var operations = transactions.SelectMany(t => t.Actions);
                           if (!operations.Any())
                           {
                               return Task.CompletedTask;
                           }
                           var client = new TableClient(_connectionString, _tableName);
#if DEBUG
                           System.Diagnostics.Debug.WriteLine("Operations to submit to the pipeline: {0}", operations.Count());
#endif
                           return _retryPolicy.ExecuteAsync(async () => await client.SubmitTransactionAsync(operations));
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
            return _pipeline.SendAsync(entityTransactionGroup, cancellationToken);
        }

        public Task CommitTransactionAsync()
        {
            return (_pipeline == null) ? Task.CompletedTask : _pipeline.CompleteAsync();
        }

        public async Task SubmitAllAsync(CancellationToken cancellationToken = default)
        {
            if (_pendingOperations.Count != 0)
            {
                    var client = new TableClient(_connectionString, _tableName);
                    await _retryPolicy.ExecuteAsync(async () => await client.SubmitTransactionAsync(_pendingOperations.ToList(), cancellationToken));
                    _pendingOperations.Clear();
               }
             }
    }
}