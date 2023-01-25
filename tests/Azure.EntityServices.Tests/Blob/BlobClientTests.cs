using Azure.EntityServices.Blobs;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Common.Samples;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Azure.EntityServices.Blob.Tests
{
    [TestClass]
    public class BlobClientTests
    {
        private string TempContainerName() => $"{nameof(BlobClientTests)}{Guid.NewGuid():N}".ToLowerInvariant();

        private readonly Random _random = new();

        public BlobClientTests()
        {
        }

        [TestMethod]
        public async Task Should_Download_File()
        {
            var sampleFileContent = GenerateRandomText(255);

            // Get a connection string to our Azure Storage account.
            var connectionString = TestEnvironment.ConnectionString;
            var containerName = TempContainerName();
            var client = new BlobStorageService(new BlobStorageServiceOptions()
            {
                Container = containerName,
                ConnectionString = TestEnvironment.ConnectionString
            });
            // Get a reference to a container named "sample-container" and then create it
            var container = new BlobContainerClient(connectionString, containerName);
            await container.CreateIfNotExistsAsync();
            

            try
            {
                var fileName = GenerateRandomBlobName(nameof(Should_Download_File));
                // Get a reference to a blob named "sample-file"
                var blob = container.GetBlobClient(fileName);

                // First upload something the blob so we have something to download
                await using (var fs = GenerateBlob(sampleFileContent))
                {
                    await blob.UploadAsync(fs);
                }

                await using var downloadedFs = await client.DownloadAsync(fileName);
                downloadedFs.Seek(0, SeekOrigin.Begin);
                sampleFileContent.Should().Be(await new StreamReader(downloadedFs).ReadToEndAsync());

                // Verify the contents
            }
            finally
            {
                await container.DeleteIfExistsAsync();
            }
        }

        /// <summary>
        /// List all the blobs in a container.
        /// </summary>
        [TestMethod]
        public async Task Should_List()
        {
            // Get a connection string to our Azure Storage account.
            var connectionString = TestEnvironment.ConnectionString;
            var file1 = GenerateRandomText(255);
            var file2 = GenerateRandomText(255);
            var file3 = GenerateRandomText(255);
            var blobPath = $"{nameof(Should_List)}/{GenerateRandomText(10)}";
            var containerName = TempContainerName();
            // Get a reference to a container named "sample-container" and then create it
            var container = new BlobContainerClient(connectionString, containerName);
            var blobNames = new List<string> { $"{blobPath}/{nameof(file1)}", $"{blobPath}/{nameof(file2)}", $"{blobPath}/{nameof(file3)}" };

            await container.CreateIfNotExistsAsync();
            try
            {
                // Upload a couple of blobs so we have something to list
                await container.UploadBlobAsync($"{blobPath}/{nameof(file1)}", GenerateBlob(file1));
                await container.UploadBlobAsync($"{blobPath}/{nameof(file2)}", GenerateBlob(file2));
                await container.UploadBlobAsync($"{blobPath}/{nameof(file3)}", GenerateBlob(file3));

                var client = new BlobStorageService(new BlobStorageServiceOptions()
                {
                    Container = containerName,
                    ConnectionString = TestEnvironment.ConnectionString
                });

                // List all the blobs
                var blobs = new List<IDictionary<string, string>>();
                await foreach (var page in client.ListAsync(blobPath))
                {
                    blobs.AddRange(page);
                }
                blobs.Select(b => b["_Name"]).Should().BeEquivalentTo(blobNames);
            }
            finally
            {
                await container.DeleteIfExistsAsync();
            }
        }

        /// <summary>
        /// Trigger a recoverable error.
        /// </summary>
        [TestMethod]
        public async Task Should_Raise_RequestFailedException_When_Container_Already_Exists()
        {
            // Get a connection string to our Azure Storage account.
            var connectionString = TestEnvironment.ConnectionString;

            // Get a reference to a container named "sample-container" and then create it
            var container = new BlobContainerClient(connectionString, TempContainerName());
            await container.CreateIfNotExistsAsync();

            try
            {
                // Try to create the container again
                await container.CreateAsync();
            }
            catch (RequestFailedException ex)
            {
                // Ignore any errors if the container already exists
                ex.ErrorCode.Should().Be(BlobErrorCode.ContainerAlreadyExists.ToString());

                return;
            }
            false.Should().BeTrue("Exception not raised");
            // Clean up after the test when we're finished
        }

        /// <summary>
        /// Upload a file to a blob.
        /// </summary>
        [TestMethod]
        public async Task Should_Upload_File()
        {
            var sampleFileContent = GenerateRandomText(255);

            var connectionString = TestEnvironment.ConnectionString;
            var containerName = TempContainerName();
            // Get a reference to a container named "sample-container" and then create it
            var container = new BlobContainerClient(connectionString, containerName);

            await container.CreateIfNotExistsAsync(); 
            try
            {
                // Get a reference to a blob
                var fileName = GenerateRandomBlobName(nameof(Should_Upload_File));

                var blob = container.GetBlobClient(fileName);

                var client = new BlobStorageService(new BlobStorageServiceOptions()
                {
                    Container = containerName,
                    ConnectionString = TestEnvironment.ConnectionString
                });
                // Open the file and upload its data
                await using (var fs = GenerateBlob(sampleFileContent))
                {
                    await client.UploadAsync(fileName, fs);
                }

                // Verify we uploaded some content
                BlobProperties properties = await blob.GetPropertiesAsync();
                ((long)sampleFileContent.Length).Should().Be(properties.ContentLength);
            }
            finally
            {
                await container.DeleteIfExistsAsync();
            }
        }

        /// <summary>
        /// List all the blobs in a container.
        /// </summary>
        [TestMethod]
        public async Task Should_List_By_Tags()
        {
            // Get a connection string to our Azure Storage account.
            var connectionString = TestEnvironment.ConnectionString;
            var file1 = GenerateRandomText(255);
            var containerName = TempContainerName();
            // Get a reference to a container named "sample-container" and then create it
            var container = new BlobContainerClient(connectionString, containerName);
            var blobsToUpload = new List<string>();
            var tag = Guid.NewGuid().ToString();
            await container.CreateIfNotExistsAsync();
            try
            {
                var client = new BlobStorageService(new BlobStorageServiceOptions()
                {
                    Container = containerName,
                    ConnectionString = TestEnvironment.ConnectionString
                });
                for (var i = 0; i < 11; i++)
                {
                    var name = GenerateRandomBlobName(nameof(Should_List_By_Tags));
                    blobsToUpload.Add(name);
                    await client.UploadAsync(name, GenerateBlob(file1), new Dictionary<string, string>() { ["TenantId"] = tag }, null);
                }

                // List all the blobs
                var blobs = new List<IDictionary<string, string>>();
                await Task.Delay(2000);
                await foreach (var page in client.ListByTagsAsync($"\"TenantId\" = '{tag}'"))
                {
                    blobs.AddRange(page);
                }
                blobs.Select(p => p["_Name"]).Should().BeEquivalentTo(blobsToUpload);
            }
            finally
            {
                await container.DeleteIfExistsAsync();
            }
        }

        [TestMethod]
        public async Task Should_Add_And_Get_Properties()
        {
            // Get a connection string to our Azure Storage account.
            var connectionString = TestEnvironment.ConnectionString;
            var file1 = GenerateRandomText(255);
            var containerName = TempContainerName();
            // Get a reference to a container named "sample-container" and then create it
            var container = new BlobContainerClient(connectionString, containerName);
            try
            {
                var name = $"{nameof(Should_Add_And_Get_Properties)}-{DateTime.UtcNow.Ticks}";
                var propValue = Guid.NewGuid().ToString();
                var propValue2 = Guid.NewGuid().ToString();
                await container.CreateIfNotExistsAsync();
                var client = new BlobStorageService(new BlobStorageServiceOptions()
                {
                    Container = containerName,
                    ConnectionString = TestEnvironment.ConnectionString
                });
                await client.UploadAsync(name, GenerateBlob(file1), null, new Dictionary<string, string>() { ["Property1"] = propValue, ["Property2"] = propValue2 });
                var props = await client.GetBlobProperiesAsync(name);

                props.Should().ContainKeys("Property1", "Property2");
                props.Should().ContainValues(propValue, propValue2);
            }
            finally
            {
                await container.DeleteIfExistsAsync();
            }
        }

        [TestMethod]
        public async Task Should_Add_And_Get_Header_Properties()
        {
            // Get a connection string to our Azure Storage account.
            var connectionString = TestEnvironment.ConnectionString;
            var file1 = GenerateRandomText(255);
            var containerName = TempContainerName();
            // Get a reference to a container named "sample-container" and then create it
            var container = new BlobContainerClient(connectionString, containerName);
            try
            {
                var name = $"{nameof(Should_Add_And_Get_Header_Properties)}-{DateTime.UtcNow.Ticks}";
                var contentType = "application/json";
                var contentEncoding = "gzip";
                await container.CreateIfNotExistsAsync();
                var client = new BlobStorageService(new BlobStorageServiceOptions()
                {
                    Container = containerName,
                    ConnectionString = TestEnvironment.ConnectionString
                });
                await client.UploadAsync(name, GenerateBlob(file1), null, new Dictionary<string, string>() { ["ContentType"] = contentType, ["ContentEncoding"] = contentEncoding });
                var props = await client.GetBlobProperiesAsync(name);
                props.Should().ContainKeys("ContentType", "ContentEncoding");
                props.Should().ContainValues(contentType, contentEncoding);
            }
            finally
            {
                await container.DeleteIfExistsAsync();
            }
        }

        private static Stream GenerateBlob(string content)
        {
            return new MemoryStream(Encoding.ASCII.GetBytes(content));
        }

        private string GenerateRandomBlobName(string subContainer)
        {
            return $"{subContainer}/{GenerateRandomText(10, true)}";
        }

        // Generates a random string with a given size.
        private string GenerateRandomText(int size, bool lowerCase = false)
        {
            var builder = new StringBuilder(size);
            var offset = lowerCase ? 'a' : 'A';
            const int lettersOffset = 26; // A...Z or a..z: length = 26

            for (var i = 0; i < size; i++)
            {
                var @char = (char)_random.Next(offset, offset + lettersOffset);
                builder.Append(@char);
            }

            return lowerCase ? builder.ToString().ToLower() : builder.ToString();
        }
    }
}