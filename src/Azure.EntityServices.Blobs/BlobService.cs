using Azure.EntityServices.Blobs.Extensions;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Polly;
using Polly.Retry;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Azure.EntityServices.Blobs
{
    public class BlobService : IBlobService
    {
        private static readonly IDictionary<string, PropertyInfo> HeaderProps = typeof(BlobHttpHeaders)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
             .ToDictionary(x => x.Name, x => x);

        private BlobServiceOptions _options;
        private BlobContainerClient _client;
        private BlobContainerClient _configuredClient => _client ?? throw new InvalidOperationException("BlobService was not configured");

        private readonly BlobServiceClient _blobServiceClient;
        private AsyncRetryPolicy _retryPolicy;

        public BlobService(BlobServiceClient blobServiceClient)
        {
            _blobServiceClient = blobServiceClient;
        }

        public BlobService Configure(BlobServiceOptions options)
        {
            _options = options;
            _client ??= _blobServiceClient.GetBlobContainerClient(_options.ContainerName);
            _retryPolicy = Policy.Handle<RequestFailedException>(ex => HandleExceptions(_options.ContainerName, _blobServiceClient, ex))
                             .WaitAndRetryAsync(3, i => TimeSpan.FromSeconds(1));
            return this;
        }

        public async Task<IDictionary<string, string>> GetBlobProperiesAsync(string blobRef)
        {
            var blob = _configuredClient.GetBlobClient(CleanupBasePath(blobRef));

            var props = await blob.GetPropertiesAsync();

            return props.Value?.Metadata ?? new Dictionary<string, string>();
        }

        public async Task<IDictionary<string, string>> GetBlobTagsAsync(string blobRef)
        {
            var blob = _configuredClient.GetBlobClient(CleanupBasePath(blobRef));

            var props = await blob.GetTagsAsync();

            return props.Value?.Tags ?? new Dictionary<string, string>();
        }

        public async Task<Stream> DownloadAsync(string blobRef)
        {
            var blob = _configuredClient.GetBlobClient(CleanupBasePath(blobRef));
            var download = await blob.DownloadAsync();
            var resultStream = new MemoryStream();
            await download.Value.Content.CopyToAsync(resultStream);
            resultStream.ResetPosition();
            return resultStream;
        }

        public async Task<string> DownloadAsTextAsync(string blobRef)
        {
            var blob = _configuredClient.GetBlobClient(CleanupBasePath(blobRef));
            var download = await blob.DownloadAsync();

            using var reader = new StreamReader(download.Value.Content);
            var response = await reader.ReadToEndAsync();
            return response;
        }

        public async Task<IDictionary<string, string>> FetchPropAsync(string blobRef)
        {
            var blob = _configuredClient.GetBlobClient(blobRef);
            var response = await blob.GetPropertiesAsync();
            return response.Value?.Metadata;
        }

        public IAsyncEnumerable<IReadOnlyList<IDictionary<string, string>>> ListAsync()
        {
            return ListAsync(string.Empty);
        }

        public async IAsyncEnumerable<IReadOnlyList<IDictionary<string, string>>> ListAsync(string blobPath)
        {
            await foreach (var itemPage in _configuredClient.GetBlobsAsync(BlobTraits.Metadata, prefix: blobPath)
                    .AsPages(pageSizeHint: _options.MaxResultPerPage))

            {
                yield return itemPage.Values.Select(blobItem => ExtractPropertiesFromBlob(blobItem).AsDictionnary()).ToList();
            }
        }

        public async IAsyncEnumerable<IReadOnlyList<IDictionary<string, string>>> ListByTagsAsync(string tagQuery)
        {
            await foreach (var tagItems in _blobServiceClient.FindBlobsByTagsAsync(tagQuery)
                .AsPages(pageSizeHint: _options.MaxResultPerPage))
            {
                var propItems = new List<IDictionary<string, string>>();

                foreach (var item in tagItems.Values)
                {
                    var props = await GetBlobProperiesAsync(item.BlobName);
                    props.Add("_Name", item.BlobName);
                    propItems.Add(props);
                }

                yield return propItems;
            }
        }

        public Task UploadAsync(string blobRef, Stream streamContent)
        {
            return UploadAsync(blobRef, streamContent, null, null);
        }

        public Task UploadAsync(string blobRef, string textContent)
        {
            return UploadAsync(blobRef, textContent, null, null);
        }

        public async Task UploadAsync(string blobRef, Stream streamContent, IDictionary<string, string> tags, IDictionary<string, string> props)
        {
            var blobName = CleanupBasePath(blobRef);
            var blob = _configuredClient.GetBlobClient(blobName);

            await _retryPolicy.ExecuteAsync(async () =>
            {
                streamContent.ResetPosition();
                await blob.UploadAsync(streamContent, overwrite: true);
            });

            if (tags != null && tags.Any())
            {
                await blob.SetTagsAsync(tags);
            }
            if (props != null && props.Any())
            {
                await blob.SetMetadataAsync(props);
                await SetHeaders(blob, props);
            }
        }

        public async Task UploadAsync(string blobRef, string textContent, IDictionary<string, string> tags, IDictionary<string, string> props)
        {
            _ = blobRef ?? throw new ArgumentNullException(nameof(blobRef));

            // Open the file and upload its data
            using var stream = CreateStreamFromText(textContent);

            await UploadAsync(blobRef, stream, tags, props);
        }

        public async Task MoveAsync(string sourceBlobRef, string destBlobRef)
        {
            _ = sourceBlobRef ?? throw new ArgumentNullException(nameof(sourceBlobRef));
            _ = destBlobRef ?? throw new ArgumentNullException(nameof(destBlobRef));

            using var file = await DownloadAsync(sourceBlobRef);
            await UploadAsync(destBlobRef, file);
            await DeleteAsync(sourceBlobRef);
        }

        public Task DeleteAsync(string blobRef)
        {
            return _configuredClient.DeleteBlobIfExistsAsync(blobRef);
        }

        public Task DeleteContainerAsync()
        {
            return _blobServiceClient.DeleteBlobContainerAsync(_options.ContainerName);
        }

        private static Stream CreateStreamFromText(string content)
        {
            return new MemoryStream(Encoding.UTF8.GetBytes(content));
        }

        private static string CleanupBasePath(string filePath)
        {
            return filePath.StartsWith("/", StringComparison.InvariantCultureIgnoreCase) ? filePath.Remove(0, 1) : filePath;
        }

        private static Task SetHeaders(BlobClient blob, IDictionary<string, string> props)
        {
            BlobHttpHeaders headers = null;
            foreach (var prop in props.Where(p => HeaderProps.ContainsKey(p.Key)))
            {
                headers ??= new BlobHttpHeaders();
                HeaderProps[prop.Key].SetValue(headers, prop.Value);
            }
            if (headers != null)
            {
                return blob.SetHttpHeadersAsync(headers);
            }

            return Task.CompletedTask;
        }

        private static IEnumerable<KeyValuePair<string, string>> ExtractPropertiesFromBlob(BlobItem blobItem)
        {
            yield return new KeyValuePair<string, string>($"_{nameof(blobItem.Name)}", blobItem.Name);
            yield return new KeyValuePair<string, string>($"_{nameof(blobItem.Properties.ContentType)}", blobItem.Properties.ContentType);
            yield return new KeyValuePair<string, string>($"_{nameof(blobItem.Properties.ContentEncoding)}", blobItem.Properties.ContentEncoding);
            yield return new KeyValuePair<string, string>($"_{nameof(blobItem.Properties.ContentLength)}", blobItem.Properties.ContentLength.ToInvariantString());

            foreach (var prop in blobItem.Metadata)
            {
                yield return prop;
            }
        }

        private static bool HandleExceptions(string containerName, BlobServiceClient serviceClient, RequestFailedException exception)
        {
            if (exception?.ErrorCode == "ContainerNotFound")
            {
                serviceClient.CreateBlobContainer(containerName);
                return true;
            }
            return false;
        }
    }
}