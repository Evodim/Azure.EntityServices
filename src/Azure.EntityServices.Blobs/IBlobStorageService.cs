using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace Azure.EntityServices.Blobs
{
    public interface IBlobService
    {
        Task UploadAsync(string blobRef, Stream streamContent);

        Task UploadAsync(string blobRef, string textContent);

        Task UploadAsync(string blobRef, Stream streamContent, IDictionary<string, string> tags, IDictionary<string, string> props);

        IAsyncEnumerable<IReadOnlyList<IDictionary<string, string>>> ListAsync();

        IAsyncEnumerable<IReadOnlyList<IDictionary<string, string>>> ListAsync(string blobPath);

        IAsyncEnumerable<IReadOnlyList<IDictionary<string, string>>> ListByTagsAsync(string tagQuery);

        Task<Stream> DownloadAsync(string blobRef);

        Task MoveAsync(string sourceBlobRef, string destBlobRef);

        Task DeleteAsync(string blobRef);

        Task<string> DownloadAsTextAsync(string blobRef);

        Task<IDictionary<string, string>> GetBlobProperiesAsync(string blobRef);

        Task<IDictionary<string, string>> GetBlobTagsAsync(string blobRef);
    }
}