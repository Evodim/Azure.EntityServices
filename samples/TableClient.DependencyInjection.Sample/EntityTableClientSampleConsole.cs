using Azure.EntityServices.Tables;
using Azure.EntityServices.Tables.Extensions.DependencyInjection;
using Common.Samples.Models;
using Common.Samples.Tools.Fakes;
using Microsoft.Extensions.Azure;
using Microsoft.Extensions.Hosting;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace TableClient.DependencyInjection.Sample
{
    public class EntityTableClientSampleConsole : IHostedService
    {
        private const int ENTITY_COUNT = 100;
        private readonly IEntityTableClient<PersonEntity> _defaultClient;
        private readonly IEntityTableClient<PersonEntity> _namedEntityTableClient;
        private readonly IEntityTableClient<PersonEntity> _namedEntityTableClient2;

        public EntityTableClientSampleConsole(
            IEntityTableClient<PersonEntity> defaultClient,
            IAzureClientFactory<IEntityTableClient<PersonEntity>> factory)
        {
            _defaultClient = defaultClient;
            _namedEntityTableClient = factory.CreateClient($"{nameof(PersonEntity)}1");
            _namedEntityTableClient2 = factory.CreateClient($"{nameof(PersonEntity)}2");
        }

        public async Task Run()
        {
            //define tenant values as table partitions
            var tenants = new string[] { "tenantX", "tenant1", "tenant2", "tenant3", "tenant4", "tenant5" };

            var faker = Fakers.CreateFakePerson(tenants);

            Console.Write($"Generate faked {ENTITY_COUNT} entities...");
            var entities = faker.Generate(ENTITY_COUNT);

            Console.WriteLine($"Add {ENTITY_COUNT} entities...");

            await _defaultClient.AddManyAsync(entities);
            await _namedEntityTableClient.AddManyAsync(entities);
            await _namedEntityTableClient2.AddManyAsync(entities);
         
            var person = await _namedEntityTableClient.GetByIdAsync(entities.Last()?.TenantId, entities.Last()?.PersonId);

            Console.WriteLine($"Readed {person?.TenantId} {person?.PersonId} ");

            person = await _namedEntityTableClient2.GetByIdAsync(entities.Last()?.TenantId, entities.Last()?.PersonId);

            Console.WriteLine($"Readed {person?.TenantId} {person?.PersonId} ");

            person = await _defaultClient.GetByIdAsync(entities.Last()?.TenantId, entities.Last()?.PersonId);

            Console.WriteLine($"Readed {person?.TenantId} {person?.PersonId} ");
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