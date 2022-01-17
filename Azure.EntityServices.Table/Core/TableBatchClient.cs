using Azure.Data.Tables;
using Polly.Retry;
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Azure.EntityServices.Table.Core
{

    public class TableBatchClient
    {
        private readonly AsyncRetryPolicy _retryPolicy;
        private readonly ConcurrentQueue<TableTransactionAction> _pendingOperations;
        private readonly string _tableName;
        private readonly string _connectionString;
        private readonly IPipeline _pipeline; 

        public TableBatchClient(
            TableBatchClientOptions options,
            AsyncRetryPolicy retryPolicy)
        {
            _ = options ?? throw new ArgumentNullException(nameof(options));
            _ = retryPolicy ?? throw new ArgumentNullException(nameof(retryPolicy));

            _pendingOperations = new ConcurrentQueue<TableTransactionAction>();
            _connectionString = options.ConnectionString;
            _tableName = options.TableName;
            _retryPolicy = retryPolicy;
            //Create en configure transaction entity group flow pipepline
            _pipeline = CustomTplBlocks.CreatePipeline(async transactions =>
            {
                var client = new TableClient(_connectionString, _tableName); 
                await _retryPolicy.ExecuteAsync(async () => await client.SubmitTransactionAsync(transactions.SelectMany(t => t.Actions))); 
            }, options.MaxItemPerTransaction, options.MaxParallelTasks);
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

        public Task AddToTransactionAsync(string partitionKey, string primaryKey, CancellationToken cancellationToken = default)
        {
            var entityTransactionGroup = new EntityTransactionGroup(partitionKey, primaryKey);
            entityTransactionGroup.Actions.AddRange(_pendingOperations.ToList());
            _pendingOperations.Clear();
            return _pipeline.SendAsync(entityTransactionGroup, cancellationToken);
        }

        public Task CommitTransactionAsync()
        {
            return _pipeline.CompleteAsync();
        }

        public Task ExecuteAsync(CancellationToken cancellationToken = default)
        { 
            if (_pendingOperations.Count != 0)
            {
                var client = new TableClient(_connectionString, _tableName);
                return _retryPolicy.ExecuteAsync(async () => await client.SubmitTransactionAsync(_pendingOperations, cancellationToken));
            }
            return Task.CompletedTask;
        }
    }
}