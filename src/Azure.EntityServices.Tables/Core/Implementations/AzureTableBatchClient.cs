using Azure.Data.Tables;
using Azure.EntityServices.Tables.Core.Abstractions;
using Azure.EntityServices.Tables.Extensions;
using Polly;
using Polly.Retry;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Azure.EntityServices.Tables.Core.Implementations
{
    public class AzureTableBatchClient<T> : BaseTableBatchClient<T> where T : class, new()
    {
        private readonly AsyncRetryPolicy _retryPolicy;
        private readonly TableClient _client;
        private readonly TableServiceClient _tableClientService;

        public AzureTableBatchClient(
            TableServiceClient tableServiceClient,
            TableBatchClientOptions options,
            Func<EntityTransactionGroup, Task<EntityTransactionGroup>> preProcessor,
            IEntityAdapter<T> entityAdapter,
            Func<IEnumerable<EntityOperation>, Task> onTransactionSubmittedHandler = null) : base(
                options,
                preProcessor,
                entityAdapter,
                onTransactionSubmittedHandler)
        {
            _tableClientService = tableServiceClient;

            _retryPolicy = Policy.Handle<RequestFailedException>(ex => ex.HandleAzureStorageException(options.TableName, tableServiceClient, options.CreateTableIfNotExists))
              .WaitAndRetryAsync(5, i => TimeSpan.FromSeconds(2 * i));

            _client = _tableClientService.GetTableClient(options.TableName);
        }

        private Task SendUniqueOperation(EntityOperation entityOperation, CancellationToken cancellationToken = default)
        {
            var nativeEntity = entityOperation.ToTableEntityModel<T>();
            return _retryPolicy.ExecuteAsync(async () =>
            {
                switch (entityOperation.EntityOperationType)
                {
                    case EntityOperationType.Add:
                        await _client.AddEntityAsync(nativeEntity, cancellationToken);
                        break;

                    case EntityOperationType.Replace:
                        await _client.UpdateEntityAsync(nativeEntity, ETag.All, mode: TableUpdateMode.Replace, cancellationToken: cancellationToken);
                        break;

                    case EntityOperationType.Merge:
                        await _client.UpdateEntityAsync(nativeEntity, ETag.All, mode: TableUpdateMode.Merge, cancellationToken: cancellationToken);
                        break;

                    case EntityOperationType.AddOrReplace:
                        await _client.UpsertEntityAsync(nativeEntity, mode: TableUpdateMode.Replace, cancellationToken: cancellationToken);
                        break;

                    case EntityOperationType.AddOrMerge:
                        await _client.UpsertEntityAsync(nativeEntity, mode: TableUpdateMode.Merge, cancellationToken: cancellationToken);
                        break;

                    default: throw new NotSupportedException(nameof(entityOperation.EntityOperationType));
                }
            });
        }

        protected override Task SubmitTransaction(IEnumerable<EntityOperation> entityOperations, CancellationToken cancellationToken = default)
        {
            if (entityOperations.Count() == 1)
            {
                return _retryPolicy.ExecuteAsync(() =>
                SendUniqueOperation(entityOperations.First(), cancellationToken));
            }

            return _retryPolicy.ExecuteAsync(() =>
            _client.SubmitTransactionAsync(entityOperations.Select(
                op =>
                new TableTransactionAction(op.EntityOperationType.MapToTableTransactionActionType(),
                                           op.ToTableEntityModel<T>())
            ), cancellationToken));
        }
    }
}