using Azure.Data.Tables;
using Azure.EntityServices.Queries;
using Azure.EntityServices.Tables.Core.Abstractions;
using Azure.EntityServices.Tables.Extensions;
using Polly;
using Polly.Retry;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Azure.EntityServices.Tables.Core.Implementations
{
    public class AzureNativeTableClient<T> : INativeTableClient<T> where T : class, new()
    {
        private readonly IEntityAdapter<T> _entityAdapter;
        private readonly RetryPolicy _retryPolicy;
        private readonly AsyncRetryPolicy _asyncRetryPolicy;
        private readonly TableClient _tableClient;

        public AzureNativeTableClient(
            EntityTableClientOptions options,
            IEntityAdapter<T> entityAdapter,
            TableServiceClient tableServiceClient)
        {
            _entityAdapter = entityAdapter;

            _retryPolicy = Policy.Handle<RequestFailedException>(ex => ex.HandleAzureStorageException(options.TableName, tableServiceClient, options.CreateTableIfNotExists))
               .WaitAndRetry(5, i => TimeSpan.FromSeconds(2 * i));

            _asyncRetryPolicy = Policy.Handle<RequestFailedException>(ex => ex.HandleAzureStorageException(options.TableName, tableServiceClient, options.CreateTableIfNotExists))
              .WaitAndRetryAsync(5, i => TimeSpan.FromSeconds(2 * i));

            _tableClient = tableServiceClient.GetTableClient(options.TableName);
        }

        private IAsyncEnumerable<Page<TableEntity>> QueryTableEntities(Action<IQuery<T>> filter, int? maxPerPage, string nextPageToken, bool? iterateOnly = false, CancellationToken cancellationToken = default)
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

            var strQuery = new AzureTableStorageQueryBuilder<T>(query).Build();

            return _retryPolicy.Execute(() => _tableClient
                                                         .QueryAsync<TableEntity>(filter: string.IsNullOrWhiteSpace(strQuery) ? null : strQuery, select: iterateOnly == true ? new string[] { "PartitionKey", "RowKey" } : null,
                                                          cancellationToken: cancellationToken,
                                                           maxPerPage: maxPerPage)
                                                         .AsPages(nextPageToken));
        }

        public async Task<bool> CreateTableIfNotExists(CancellationToken cancellationToken = default)
        {
            var response = await _tableClient.CreateIfNotExistsAsync(cancellationToken);
            return response != null;
        }

        public async Task<bool> DropTableIfExists(CancellationToken cancellationToken = default)
        {
            var response = await _tableClient.DeleteAsync(cancellationToken);
            return response.IsError;
        }

        public async IAsyncEnumerable<EntityPage<T>> QueryEntities(
            Action<IQuery<T>> filter,
            int? maxPerPage,
            string nextPageToken,
            bool? iterateOnly = false,
            [EnumeratorCancellation] CancellationToken cancellationToken = default
            )
        {
            var iteratedCount = 0;
            await foreach (var page in QueryTableEntities(filter, maxPerPage, nextPageToken, iterateOnly, cancellationToken))
            {
                yield return new EntityPage<T>(
                    page.Values.Select(tableEntity => _entityAdapter.FromEntityModel(tableEntity)),
                    iteratedCount += page.Values.Count,
                    string.IsNullOrWhiteSpace(page.ContinuationToken),
                    page.ContinuationToken);
            }
        }

        public async Task<IDictionary<string, object>> GetEntityProperties(string partitionKey, string rowKey, IEnumerable<string> properties = null, CancellationToken cancellationToken = default)
        {
            var nativeEntity = await _asyncRetryPolicy.ExecuteAsync(async () => await _tableClient.GetEntityIfExistsAsync<TableEntity>(partitionKey, rowKey, properties));
            if (nativeEntity.HasValue && nativeEntity.Value != null)
            {
                return nativeEntity.Value;
            }
            return null;
        }

        public Task SubmitOneOperation(EntityOperation entityOperation, CancellationToken cancellationToken = default)
        {
            var nativeEntity = entityOperation.ToTableEntityModel<T>();
            return _asyncRetryPolicy.ExecuteAsync(async () =>
            {
                switch (entityOperation.EntityOperationType)
                {
                    case EntityOperationType.Add:
                        await _tableClient.AddEntityAsync(nativeEntity, cancellationToken);
                        break;

                    case EntityOperationType.Replace:
                        await _tableClient.UpdateEntityAsync(nativeEntity, ETag.All, mode: TableUpdateMode.Replace, cancellationToken: cancellationToken);
                        break;

                    case EntityOperationType.Merge:
                        await _tableClient.UpdateEntityAsync(nativeEntity, ETag.All, mode: TableUpdateMode.Merge, cancellationToken: cancellationToken);
                        break;

                    case EntityOperationType.AddOrReplace:
                        await _tableClient.UpsertEntityAsync(nativeEntity, mode: TableUpdateMode.Replace, cancellationToken: cancellationToken);
                        break;

                    case EntityOperationType.AddOrMerge:
                        await _tableClient.UpsertEntityAsync(nativeEntity, mode: TableUpdateMode.Merge, cancellationToken: cancellationToken);
                        break;

                    case EntityOperationType.Delete:
                        await _tableClient.DeleteEntityAsync(nativeEntity.PartitionKey, nativeEntity.RowKey, cancellationToken: cancellationToken);
                        break;

                    default: throw new NotSupportedException(nameof(entityOperation.EntityOperationType));
                }
            });
        }

        public Task SubmitTransaction(IEnumerable<EntityOperation> entityOperations, CancellationToken cancellationToken = default)
        {
            if (entityOperations.Count() == 1)
            {
                return SubmitOneOperation(entityOperations.First(), cancellationToken);
            }

            return _asyncRetryPolicy.ExecuteAsync(() =>
            _tableClient.SubmitTransactionAsync(entityOperations.Select(
                op =>
                new TableTransactionAction(op.EntityOperationType.MapToTableTransactionActionType(),
                                           op.ToTableEntityModel<T>())
            ), cancellationToken));
        }
    }
}
