using Azure.EntityServices.Blobs;
using Common.Samples;
using Common.Samples.Diagnostics;
using Common.Samples.Models;
using Common.Samples.Tools;
using Common.Samples.Tools.Fakes;
using System;
using System.Threading.Tasks;

namespace BlobClient.BasicSample
{
    public static class BlobSample
    {
        private const int ENTITY_COUNT = 50;

        public static async Task Run()
        {
            var options = new EntityBlobClientOptions($"{nameof(DocumentEntity)}Container".ToLower());

            //Configure entity binding in the table storage
            var client = EntityBlobClient.Create<DocumentEntity>(TestEnvironment.ConnectionString)
                .Configure(options, config =>
             config
                .SetBlobContentProp(p => p.Content)
                .SetBlobPath(p => $"{p.Created:yyyy/MM/dd}")
                .SetBlobName(p => $"{p.Name}-{p.Reference}.{p.Extension}")
                .AddTag(p => p.Reference)
                .AddTag(p => p.Name));

            var faker = Fakers.CreateFakedDoc();

            Console.Write($"Generate faked {ENTITY_COUNT} entities...");
            var entities = faker.Generate(ENTITY_COUNT);
            Console.WriteLine("OK");

            var counters = new PerfCounters(nameof(EntityBlobClient<PersonEntity>));
            Console.Write($"Insert {ENTITY_COUNT} entities...");

            using (var mesure = counters.Mesure($"{ENTITY_COUNT} insertions"))
            {
                foreach (var entity in entities)
                {
                    await client.AddOrReplaceAsync(entity);
                }
            }
            using (var mesure = counters.Mesure($"readed"))
            {
                var count = 0;
                await foreach (var readed in client.ListAsync($"{DateTimeOffset.UtcNow:yyyy/MM/dd}"))
                {
                    foreach (var entity in readed)
                    {
                        Console.WriteLine($"{client.GetEntityReference(entity)}");
                        count++;
                    }
                }
                Console.WriteLine($"Readed : {count}");
            }

            Console.WriteLine("====================================");
            counters.WriteToConsole();
        }
    }
}