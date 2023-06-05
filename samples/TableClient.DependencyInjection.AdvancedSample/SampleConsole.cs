using Azure.EntityServices.Queries;
using Azure.EntityServices.Tables;
using Common.Samples.Models;
using Common.Samples.Tools.Fakes;
using Microsoft.Extensions.Azure;
using Microsoft.Extensions.Hosting;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace TableClient.DependencyInjection.AdvancedSample
{
    public class SampleConsole : IHostedService
    {
        private const int ENTITY_COUNT = 50;
        private readonly IEntityTableClient<PersonEntity> _defaultClient;
        private readonly IEntityTableClient<PersonEntity> _projectionClient;

        public SampleConsole(
            IEntityTableClient<PersonEntity> defaultClient,
            IAzureClientFactory<IEntityTableClient<PersonEntity>> projectionClient
            )
        {
            _defaultClient = defaultClient;
            _projectionClient = projectionClient.CreateClient(nameof(SampleProjectionObserver));
        }

        public async Task Run()
        {
            //define tenant values as table partitions
            var tenants = new string[] { "tenant1", "tenant2", "tenant3", "tenant4", "tenant5" };

            var faker = Fakers.CreateFakePerson(tenants);

            Console.Write($"Generate faked {ENTITY_COUNT} entities...");
            var entities = faker.Generate(ENTITY_COUNT);

            Console.WriteLine($"Add {ENTITY_COUNT} entities...");

            await _defaultClient.AddManyAsync(entities);

            foreach (var entity in entities)
            {
                Console.WriteLine($"from source table: {entity.LastName}");
            }

            await foreach (var batch in _projectionClient.GetAsync(filter =>
            filter
            .IncludeTags()
            .WherePartitionKey()
            .GreaterThanOrEqual("~")))
            {
                foreach (var entity in batch)
                {
                    Console.WriteLine($"from partionned projection: {entity.LastName}");
                }
            }
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            return Run();
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }
}