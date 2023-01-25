using Azure.EntityServices.Tables;
using Common.Samples.Models;
using Common.Samples.Tools.Fakes;
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
        private readonly IEntityTableClient<PersonEntity> _entityTableClient;

        public EntityTableClientSampleConsole(IEntityTableClient<PersonEntity> entityTableClient)
        {
            _entityTableClient = entityTableClient;
        }

        public async Task Run()
        {
            //define tenant values as table partitions
            var tenants = new string[] { "tenantX", "tenant1", "tenant2", "tenant3", "tenant4", "tenant5" };

            var faker = Fakers.CreateFakePerson(tenants);

            Console.Write($"Generate faked {ENTITY_COUNT} entities...");
            var entities = faker.Generate(ENTITY_COUNT);

            Console.Write($"Add {ENTITY_COUNT} entities...");

            await _entityTableClient.AddManyAsync(entities);

            var person = await _entityTableClient.GetByIdAsync(entities.Last()?.TenantId, entities.Last()?.PersonId);

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