using Azure.EntityServices.Tables;
using Common.Samples.Models;
using Common.Samples.Tools.Fakes;
using Microsoft.Extensions.Azure;
using Microsoft.Extensions.Hosting;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace TableClient.DependencyInjection.ProjectionSample
{
    /// <summary>
    /// Inject two EntityTableClient on same Entity and table to create additional indexed partition from specific field
    /// LastName value was added in batch and ordered because we add a tag to LastName field
    /// </summary>
    public class SampleConsole : IHostedService
    {
        private const int ENTITY_COUNT = 1000;
        private readonly IEntityTableClient<PersonEntity> _entityClient;
        private readonly IEntityTableClient<PersonEntity> _projectionClient;

        public SampleConsole(IAzureClientFactory<IEntityTableClient<PersonEntity>> entityClientFactory)
        {
            _entityClient = entityClientFactory.CreateClient("Source");
            _projectionClient = entityClientFactory.CreateClient("Projection");
        }

        public async Task Run()
        {
            var fakePersons = Fakers.CreateFakePerson(new string[] { "tenant1", "tenant2", "tenant3", "tenant4", "tenant5" });

            Console.Write($"Generate faked {ENTITY_COUNT} entities...");
            var persons = fakePersons.Generate(ENTITY_COUNT);
            var onePerson = fakePersons.Generate(1).First();
            onePerson.Address.State = "France";

            Console.WriteLine("OK");
            Console.Write($"Adding entities...");

            await _entityClient.AddOrReplaceManyAsync(persons);

            var count = 0;
            await foreach (var result in _entityClient
                .GetAsync(filter => filter
                    .WithTag(p => p.LastName)))

            {
                foreach (var person in result)
                {
                    Console.WriteLine(person.LastName);
                }
                count += result.Count();
                await _projectionClient.AddOrReplaceManyAsync(result);
            }
            Console.WriteLine($"updated projection entities: {count}");
            Console.WriteLine("====================================");
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