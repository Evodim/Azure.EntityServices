using Azure.EntityServices.Queries;
using Azure.EntityServices.Tables;
using Common.Samples.Models;
using Common.Samples.Tools.Fakes;
using Microsoft.Extensions.Hosting;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace TableClient.DependencyInjection.BasicSample
{
    public class SampleConsole : IHostedService
    {
        private const int ENTITY_COUNT = 50;
        private readonly IEntityTableClient<PersonEntity> _entityClient;

        public SampleConsole(IEntityTableClient<PersonEntity> entityClient)
        {
            _entityClient = entityClient;
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
            await _entityClient.AddOrReplaceAsync(onePerson);

            Console.WriteLine($"Get entity by ID");

            _ = await _entityClient.GetByIdAsync(onePerson.TenantId, onePerson.PersonId);

            Console.WriteLine($"Querying entities");
            await foreach (var filteredPersons in _entityClient.GetAsync(
                    filter => filter
                    .WherePartitionKey()
                    .Equal(onePerson.TenantId)
                    .And(p => p.LastName)
                    .Equal(onePerson.LastName)
                    .And("_IsInFrance")
                    .Equal(true)
                    ))
            {
                foreach (var entity in filteredPersons)
                {
                    Console.WriteLine($"found: {entity.LastName} {entity.FirstName}");
                }
                Console.WriteLine($"finding...");
                Console.CursorTop--;
            }

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