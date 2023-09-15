using Azure.Data.Tables;
using Azure.EntityServices.Queries;
using Azure.EntityServices.Tables.Core;
using Azure.EntityServices.Tables.Core.Abstractions;
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
    public class AzureTableClient<T> : INativeTableClient<T> where T : class, new()
    {
        private readonly EntityTableClientOptions _options;
        private readonly TableServiceClient _tableServiceClient;
        private readonly IEntityAdapter<T> _entityAdapter;
        private readonly RetryPolicy _retryPolicy;
        private readonly AsyncRetryPolicy _asyncRetryPolicy;
        private readonly TableClient _tableClient;

        public AzureTableClient(
            EntityTableClientOptions options,
            IEntityAdapter<T> entityAdapter,
            TableServiceClient tableServiceClient)
        {
            _options = options;
            _tableServiceClient = tableServiceClient;
            _entityAdapter = entityAdapter;

            _retryPolicy = Policy.Handle<RequestFailedException>(ex => ex.HandleAzureStorageException(options.TableName, tableServiceClient, options.CreateTableIfNotExists))
               .WaitAndRetry(5, i => TimeSpan.FromSeconds(2 * i));

            _asyncRetryPolicy = Policy.Handle<RequestFailedException>(ex => ex.HandleAzureStorageException(options.TableName, tableServiceClient, options.CreateTableIfNotExists))
              .WaitAndRetryAsync(5, i => TimeSpan.FromSeconds(2 * i));

            _tableClient = tableServiceClient.GetTableClient(options.TableName);
        }

        private IAsyncEnumerable<Page<TableEntity>> QueryTableEntities(Action<IQuery<T>> filter, int? maxPerPage, string nextPageToken, CancellationToken cancellationToken, bool? iterateOnly = false)
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

            var strQuery = new TableStorageQueryBuilder<T>(query).Build();

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
            [EnumeratorCancellation] CancellationToken cancellationToken,
            bool? iterateOnly = false)
        {
            var iteratedCount = 0;
            await foreach (var page in QueryTableEntities(filter, maxPerPage, nextPageToken, cancellationToken, iterateOnly))
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
    }
}