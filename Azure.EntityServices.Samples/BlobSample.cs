using Azure.EntityServices.Blobs;
using Azure.EntityServices.Samples.Diagnostics;
using Azure.EntityServices.Tests.Common;
using Azure.EntityServices.Tests.Common.Fakes;
using Azure.EntityServices.Tests.Common.Models;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Azure.EntityServices.Samples
{
    public static class BlobSample
    {
        private const int ENTITY_COUNT = 50;  

        public static async Task Run()
        {
            var options = new EntityBlobClientOptions(TestEnvironment.ConnectionString,
                $"{nameof(DocumentEntity)}Container".ToLower());

            //Configure entity binding in the table storage
            var client = new EntityBlobClient<DocumentEntity>(options, config =>
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
                    }
                }
                Console.WriteLine($"Readed : {count}");
            }

            foreach (var counter in counters.Get().OrderBy(c => c.Key))
            {
                WriteLineDuration($"{counter.Key} ", counter.Value);
            }
            Console.WriteLine("Finished"); 
        } 
        private static void WriteLineDuration(string text, IPerfCounter counter)
        {
            Console.Write(text);

            var prevColor = Console.ForegroundColor;
            Console.ForegroundColor = (counter.AverageDuration.TotalSeconds < 1) ? ConsoleColor.Green : ConsoleColor.Yellow;
            Console.WriteLine($"{Math.Round(counter.AverageDuration.TotalSeconds, 3)} seconds");

            Console.ForegroundColor = prevColor;
        }
    }
}