using Azure.EntityServices.Queries;
using Azure.EntityServices.Tables;
using Common.Samples.Models;
using Common.Samples.Tools.Fakes;
using Microsoft.Extensions.Hosting;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace TableClient.LegacySample
{
    public class SampleConsole : IHostedService
    {
        private const int ENTITY_COUNT = 200;
        private readonly IEntityTableClient<PersonEntity> _entityClient;

        public SampleConsole(IEntityTableClient<PersonEntity> entityClient)
        {
            _entityClient = entityClient;
        }

        public async Task Run()
        {
            var fakePersons = Fakers.CreateFakePerson(new string[] { "tenant1", "tenant2", "tenant3", "tenant4", "tenant5" });
            var onePerson = fakePersons.Generate(1).FirstOrDefault();

            Console.Write($"Generate faked {ENTITY_COUNT} entities...");
            var entities = fakePersons.Generate(ENTITY_COUNT);
            Console.WriteLine("OK");

            Console.Write($"Adding entities...");

            await _entityClient.AddAsync(onePerson);

            await _entityClient.AddOrReplaceAsync(onePerson);

            await _entityClient.AddManyAsync(entities);

            await _entityClient.AddOrReplaceManyAsync(entities);

            Console.WriteLine($"Querying entities ...");

            _ = await _entityClient.GetByIdAsync(onePerson.TenantId, onePerson.PersonId);

            var count = 0;
            await foreach (var _ in _entityClient.GetAsync(
                   filter => filter
                    .Where(p => p.LastName)
                    .Equal(onePerson.LastName)
                    .AndPartitionKey()
                    .Equal("tenant1"))
                    )
            {
                count += _.Count();
                Console.WriteLine($"{count} iterated");
                Console.CursorTop--;
            }
            Console.WriteLine();

            count = 0;
            await foreach (var _ in _entityClient.GetAsync(
                filter => filter
                .WhereTag(p => p.LastName)
                .Equal(onePerson.LastName)
                .AndPartitionKey()
                .Equal("tenant1"))
                )

            {
                count += _.Count();
                Console.WriteLine($"{count} iterated");
                Console.CursorTop--;
            }
            Console.WriteLine();

            count = 0;
            await foreach (var _ in _entityClient.GetAsync(
                    filter => filter
                    .WherePartitionKey()
                    .Equal("tenant1")
                    .And("_FirstLastName3Chars")
                    .Equal("arm")))
            {
                count += _.Count();
                Console.WriteLine($"{count} iterated");
                Console.CursorTop--;
            }
            Console.WriteLine();

            count = 0;
            await foreach (var _ in _entityClient.GetAsync(
                filter => filter
                .WhereTag("_FirstLastName3Chars")
                .Equal("arm")
                .AndPartitionKey()
                .Equal("tenant1")))
            {
                count += _.Count();
                Console.WriteLine($"{count} iterated");
                Console.CursorTop--;
            }

            Console.WriteLine("==============Finished======================");
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