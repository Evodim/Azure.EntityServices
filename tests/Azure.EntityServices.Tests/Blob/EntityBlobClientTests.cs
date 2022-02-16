using Azure.EntityServices.Blobs;
using Azure.EntityServices.Blobs.Extensions;
using Azure.EntityServices.Queries;
using Azure.EntityServices.Table.Common.Fakes;
using Azure.EntityServices.Table.Common.Models;
using Azure.EntityServices.Tests.Common;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Azure.EntityServices.Blob.Tests
{
    [TestClass]
    public class EntityBlobClientTests
    {
        private static EntityBlobClientOptions CreateDefaultOptions<T>()
        {
            return new EntityBlobClientOptions()
            {
                Container = $"{typeof(T).Name}{DateTime.UtcNow.Ticks}".ToLowerInvariant(),
                ConnectionString = TestEnvironment.ConnectionString
            };
        }

        public EntityBlobClientTests()
        {
        }

        [TestMethod]
        public async Task Should_Add_Then_Get_Entity()
        {
            var doc = Fakers.CreateFakedDoc().Generate(1).First();
            var client = new EntityBlobClient<DocumentEntity>(CreateDefaultOptions<DocumentEntity>(), config =>
                config
                 .SetBlobContentProp(p => p.Content)
                 .SetBlobPath(p => $"{nameof(DocumentEntity)}/{p.Created:yyyy/MM/dd}")
                 .SetBlobName(p => $"{p.Name}-{p.Reference}.{p.Extension}")
                 .AddTag(p => p.Reference)
                 .AddTag(p => p.Name));

            await client.AddOrReplaceAsync(doc);
            var reference = client.GetEntityReference(doc);
            var result = await client.GetAsync(reference);

            doc.Should().BeEquivalentTo(result, options => options.Excluding(e => e.Content));
        }

        [TestMethod]
        public async Task Should_Add_Then_Get_Entities()
        {
            var docs = Fakers.CreateFakedDoc().Generate(10);
            var client = new EntityBlobClient<DocumentEntity>(CreateDefaultOptions<DocumentEntity>(), config =>
               config
                .SetBlobContentProp(p => p.Content)
                .SetBlobPath(p => $"{nameof(DocumentEntity)}/{p.Created:yyyy/MM/dd}")
                .SetBlobName(p => $"{p.Name}-{p.Reference}.{p.Extension}")
                .AddTag(p => p.Reference)
                .AddTag(p => p.Name));

            foreach (var doc in docs)
            {
                await client.AddOrReplaceAsync(doc);
            }
            var result = new List<DocumentEntity>();
            await foreach (var doc in client.ListAsync($"{nameof(DocumentEntity)}/{DateTimeOffset.UtcNow:yyyy/MM/dd}"))
            {
                result.AddRange(doc);
            }
            result.Count.Should().Be(docs.Count);
            result.Should().BeEquivalentTo(docs, options => options.Excluding(e => e.Content));
        }

        [TestMethod]
        public async Task Should_Get_Content()
        {
            var doc = Fakers.CreateFakedDoc().Generate(1).First();
            var client = new EntityBlobClient<DocumentEntity>(CreateDefaultOptions<DocumentEntity>(), config =>
              config
               .SetBlobContentProp(p => p.Content)
               .SetBlobPath(p => $"{nameof(DocumentEntity)}/{p.Created:yyyy/MM/dd}")
               .SetBlobName(p => $"{p.Name}-{p.Reference}.{p.Extension}")
               .AddTag(p => p.Reference)
               .AddTag(p => p.Name));

            await client.AddOrReplaceAsync(doc);
            var reference = client.GetEntityReference(doc);
            var created = await client.GetContentAsync(reference);
            doc.Content.ToMD5().Should().Be(created.ToMD5());
        }

        [TestMethod]
        public async Task Should_Store_Entity_Content_As_BinaryData()
        {
            var trace = Fakers.CreateFakedHttpTraceEntity().Generate(1).First();
            var client = new EntityBlobClient<HttpTraceEntity>(CreateDefaultOptions<HttpTraceEntity>(),

                config => config
                .SetBlobPath(e => $"traces/{trace.Timestamp.Year}")
                .SetBlobName(e => $"{trace.Name}-{trace.OperationId}")
                .AddComputedProp("MD5", e => e.Body.ToMD5()));

            await client.AddOrReplaceAsync(trace);
            var createdContent = await client.GetContentAsync(trace);

            createdContent.Should().NotBeNull();
            createdContent.ToMD5().Should().Be(createdContent.ToMD5());
        }

        [TestMethod]
        public async Task Should_Store_Entity_Content_As_String()
        {
            var trace = Fakers.CreateFakedHttpTraceEntity().Generate(1).First();
            var client = new EntityBlobClient<HttpTraceEntity>(CreateDefaultOptions<HttpTraceEntity>(),

                config => config
                .SetBlobPath(e => $"traces/{trace.Timestamp.Year}")
                .SetBlobName(e => $"{trace.Name}-{trace.OperationId}")
                .SetBlobContentProp(e => e.BodyString)
                .IgnoreProp(e => e.Body)

                );

            await client.AddOrReplaceAsync(trace);
            var createdContent = await client.GetContentAsync(trace);

            createdContent.Should().NotBeNull();
            createdContent.ToString().Should().Be(trace.BodyString);
        }

        [TestMethod]
        public async Task Should_Store_Entity_Content_As_Json()
        {
            var trace = Fakers.CreateFakedHttpTraceEntity().Generate(1).First();
            var person = Fakers.CreateFakePerson().Generate(1);
            trace.BodyObject = person;

            var client = new EntityBlobClient<HttpTraceEntity>(CreateDefaultOptions<HttpTraceEntity>(),

                config => config
                .SetBlobPath(e => $"traces/{trace.Timestamp.Year}")
                .SetBlobName(e => $"{trace.Name}-{trace.OperationId}")
                .SetBlobContentProp(e => e.BodyObject)
                .IgnoreProp(e => e.Body)

                );

            await client.AddOrReplaceAsync(trace);
            var createdContent = await client.GetContentAsync(trace);

            createdContent.Should().NotBeNull();
            var contentAsPerson = createdContent.ToObjectFromJson<List<PersonEntity>>();
            contentAsPerson.Should().BeEquivalentTo(person);
        }

        [TestMethod]
        public async Task Should_Use_Computed_Prop_With_BinaryData_Content()
        {
            var trace = Fakers.CreateFakedHttpTraceEntity().Generate(1).First();
            var client = new EntityBlobClient<HttpTraceEntity>(CreateDefaultOptions<HttpTraceEntity>(),

                config => config
                .SetBlobPath(e => $"traces/{trace.Timestamp.Year}")
                .SetBlobName(e => $"{trace.Name}-{trace.OperationId}")
                .AddComputedProp("MD5", e => e.Body.ToMD5()));

            await client.AddOrReplaceAsync(trace);
            var reference = client.GetEntityReference(trace);
            var props = await client.GetPropsAsync(reference);
            props.Should().ContainKey("MD5");
            props["MD5"].Should().Be(trace.Body.ToMD5());
        }

        [TestMethod]
        public async Task Should_List_Entities_By_Index()
        {
            var docs = Fakers.CreateFakedDoc().Generate(5);
            var client = new EntityBlobClient<DocumentEntity>(CreateDefaultOptions<DocumentEntity>(), config =>
              config
               .SetBlobContentProp(p => p.Content)
               .SetBlobPath(p => $"{nameof(DocumentEntity)}/{p.Created:yyyy/MM/dd}")
               .SetBlobName(p => $"{p.Name}-{p.Reference}.{p.Extension}")
               .AddTag(p => p.Reference)
               .AddTag(p => p.Name));
            foreach (var doc in docs)
            {
                await client.AddOrReplaceAsync(doc);
            }
            Task.Delay(2000).Wait();
            var readedEntities = new List<DocumentEntity>();
            await foreach (var doc in client.ListAsync(query: p => p.Where(p => p.Reference)
            .Equal(docs.First().Reference)))
            {
                readedEntities.AddRange(doc);
            }
            readedEntities.Count.Should().Be(1);
            readedEntities.First().Should().BeEquivalentTo(docs.First(), options => options.Excluding(e => e.Content));
        }

        [TestMethod]
        public async Task Should_Filter_Entities_With_Many_Tags()
        {
            var docs = Fakers.CreateFakedDoc().Generate(5);
            var client = new EntityBlobClient<DocumentEntity>(CreateDefaultOptions<DocumentEntity>(), config =>
               config
                .SetBlobContentProp(p => p.Content)
                .SetBlobPath(p => $"{nameof(DocumentEntity)}/{p.Created:yyyy/MM/dd}")
                .SetBlobName(p => $"{p.Name}-{p.Reference}.{p.Extension}")
                .AddTag(p => p.Reference)
                .AddTag(p => p.Name));

            foreach (var doc in docs)
            {
                await client.AddOrReplaceAsync(doc);
            }
            Task.Delay(3000).Wait();
            var readedEntities = new List<DocumentEntity>();
            await foreach (var doc in client.ListAsync(query: p => p
            .Where(p => p.Name)
            .Equal(docs.First().Name)
            .And(p => p.Reference)
            .Equal(docs.First().Reference)))

            {
                readedEntities.AddRange(doc);
            }
            readedEntities.Count.Should().Be(1);
        }

        [TestMethod]
        public async Task Should_Filter_Entities_By_Computed_Prop()
        {
            var docs = Fakers.CreateFakedDoc().Generate(3);
            docs.Last().Created = DateTimeOffset.UtcNow.AddMonths(7);
            var client = new EntityBlobClient<DocumentEntity>(CreateDefaultOptions<DocumentEntity>(), config =>
               config
                .SetBlobContentProp(p => p.Content)
                .SetBlobPath(p => $"{nameof(DocumentEntity)}/{p.Created:yyyy/MM/dd}")
                .SetBlobName(p => $"{p.Name}-{p.Reference}.{p.Extension}")
                .AddComputedProp("_CreatedSince6Month", p => p.Created > DateTimeOffset.UtcNow)
                .AddTag(p => p.Reference)
                .AddTag(p => p.Name)
                .AddTag("_CreatedSince6Month"));

            foreach (var doc in docs)
            {
                await client.AddOrReplaceAsync(doc);
            }
            Task.Delay(3000).Wait();
            var readedEntities = new List<DocumentEntity>();
            await foreach (var doc in client.ListAsync(query: p => p
                            .Where("_CreatedSince6Month")
                            .Equal(false)))

            {
                readedEntities.AddRange(doc);
            }
            readedEntities.Count.Should().Be(2);
        }

        [TestMethod]
        public async Task Should_Get_Internal_Entity_Properties()
        {
            var doc = Fakers.CreateFakedDoc().Generate(1).First();
            var client = new EntityBlobClient<DocumentEntity>(CreateDefaultOptions<DocumentEntity>(), config =>
              config
               .SetBlobContentProp(p => p.Content)
               .SetBlobPath(p => $"{nameof(DocumentEntity)}/{p.Created:yyyy/MM/dd}")
               .SetBlobName(p => $"{p.Name}-{p.Reference}.{p.Extension}")
               .AddTag(p => p.Reference)
               .AddTag(p => p.Name));
            await client.AddOrReplaceAsync(doc);

            var reference = client.GetEntityReference(doc);

            var props = await client.GetPropsAsync(reference);

            props["_EntityPath"].Should().Be($"{nameof(DocumentEntity)}/{DateTimeOffset.UtcNow:yyyy/MM/dd}");
            props["_EntityName"].Should().Be($"{doc.Name}-{doc.Reference}.{doc.Extension}");
        }
    }
}