using Azure.EntityServices.Blobs;
using Common.Samples;
using Common.Samples.Diagnostics;
using Common.Samples.Models;
using Common.Samples.Tools;
using Common.Samples.Tools.Fakes;
using Microsoft.Extensions.Azure;
using Microsoft.Extensions.Hosting;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace BlobClient.BasicSample
{
    public  class SampleConsole : IHostedService
    {
        private const int ENTITY_COUNT = 50;
        private readonly IEntityBlobClient<DocumentEntity> _entityBlobClient;
   
        public SampleConsole(
            IEntityBlobClient<DocumentEntity> entityBlobClient,
            IAzureClientFactory<IEntityBlobClient<DocumentEntity>> azureFactory)
        {
            _entityBlobClient = entityBlobClient;
            //you could also inject IAzureClientFactory to resolve mamed instances of IEntityBlobClient<T>
            _ = azureFactory.CreateClient("DocumentEntityClient1");
        }
        public  async Task Run()
        { 
            var faker = Fakers.CreateFakedDoc();

            Console.Write($"Generate faked {ENTITY_COUNT} entities...");
            var entities = faker.Generate(ENTITY_COUNT);
            Console.WriteLine("OK");

            var counters = new PerfCounters(nameof(EntityBlobClient<DocumentEntity>));
            Console.Write($"Insert {ENTITY_COUNT} entities...");

            using (var mesure = counters.Mesure($"{ENTITY_COUNT} insertions"))
            {
                foreach (var entity in entities)
                {
                    await _entityBlobClient.AddOrReplaceAsync(entity);
                }
            }
            using (var mesure = counters.Mesure($"readed"))
            {
                var count = 0;
                await foreach (var readed in _entityBlobClient.ListAsync($"{DateTimeOffset.UtcNow:yyyy/MM/dd}"))
                {
                    foreach (var entity in readed)
                    {
                        Console.WriteLine($"{_entityBlobClient.GetEntityReference(entity)}");
                        count++;
                    }
                }
                Console.WriteLine($"Readed : {count}");
            }

            Console.WriteLine("====================================");
            counters.WriteToConsole();
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            return Run();
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }
    }
}