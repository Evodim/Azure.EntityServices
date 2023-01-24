using Azure.EntityServices.Table.Common.Fakes;
using Azure.EntityServices.Tables;
using Common.Samples.Diagnostics;
using Common.Samples.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace TableClient.DependencyInjection.Sample
{
    public class EntityTableClientSampleConsole : IHostedService
    {
        private const int ENTITY_COUNT = 1000;
        private readonly IEntityTableClient<PersonEntity> _entityTableClient;

        public EntityTableClientSampleConsole(IEntityTableClient<PersonEntity> entityTableClient)
        {
            _entityTableClient = entityTableClient;
        }

        public async Task Run()
        {
            var services = new ServiceCollection();

            //define tenant values as table partitions
            var tenants = new string[] { "tenantX" }; //"tenant1", "tenant2", "tenant3", "tenant4", "tenant5" };

            var faker = Fakers.CreateFakePerson(tenants);

            Console.Write($"Generate faked {ENTITY_COUNT} entities...");
            var entities = faker.Generate(ENTITY_COUNT);
            Console.WriteLine("OK");

            var counters = new PerfCounters(nameof(EntityTableClient<PersonEntity>));
            Console.Write($"Insert {ENTITY_COUNT} entities...");

            using (var mesure = counters.Mesure($"0. Add many entities {ENTITY_COUNT} items"))
            {
                await _entityTableClient.AddManyAsync(
                    entities.Select(e =>
                    {
                        e.TenantId = $"{e.TenantId}"; return e;
                    }));
            }
            //using (var mesure = counters.Mesure($"1. Add many {ENTITY_COUNT} "))
            //{
            //    foreach (var entity in entities)
            //    {
            //        entity.TenantId = "tenantX";
            //        entity.LastName = "Replaced_" + entity.LastName;
            //        await _entityTableClient.ReplaceAsync(entity);
            //    }

            //}
            //using (var mesure = counters.Mesure($"2. Add or replace many {ENTITY_COUNT} "))
            //{
            //    foreach (var entity in entities) {
            //        entity.LastName = "*" + entity.LastName;
            //    }
            //    await _entityTableClient.AddOrReplaceManyAsync(entities);

            //}
            //using (var mesure = counters.Mesure($"3. update many {ENTITY_COUNT} "))
            //{
            //   await _entityTableClient.UpdateManyAsync(entity =>
            //    {
            //        entity.LastName = "*" + entity.LastName;
            //    }, filter => filter.WherePartitionKey().Equal("tenantX"));

            //}

            //using (var mesure = counters.Mesure($"0. Add or replace many entities {ENTITY_COUNT} items"))
            //{
            //    await foreach (var page in _entityTableClient.GetByTagAsync(f => f
            //    .WhereTag("LastName")
            //    .Between("meh ", "meh ~")
            //    .AndPartitionKey()
            //    .Equal("tenant1")))
            //    {
            //        var t = page.ToList();
            //        foreach (var person in t)
            //        {
            //            person.LastName = "mehdi " + person.LastName;
            //        }
            //        await _entityTableClient.AddOrReplaceManyAsync(t);
            //    }
            //}
            //using (var mesure = counters.Mesure($"0. Add or replace many entities {ENTITY_COUNT} items"))
            //{
            //    await foreach (var page in _entityTableClient.GetByTagAsync(f => f
            //    .WhereTag("LastName")
            //    .Between("mehdi ", "mehdi ~")
            //    .AndPartitionKey()
            //    .Equal("tenant2")))
            //    {
            //        var t = page.ToList();
            //        foreach (var person in t)
            //        {
            //            person.LastName = "mehdi " + person.LastName;
            //        }
            //        await _entityTableClient.AddOrReplaceManyAsync(t);
            //    }
            //}

            Console.WriteLine("====================================");
            counters.WriteToConsole();
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