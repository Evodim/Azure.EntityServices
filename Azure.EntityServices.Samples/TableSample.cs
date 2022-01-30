﻿using Azure.EntityServices.Queries;
using Azure.EntityServices.Samples.Diagnostics;
using Azure.EntityServices.Tables;
using Azure.EntityServices.Tests.Common;
using Azure.EntityServices.Tests.Common.Fakes;
using Azure.EntityServices.Tests.Common.Models;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Azure.EntityServices.Samples
{
    public static class TableSample
    {
        private const int ENTITY_COUNT = 500;
        public static async Task Run()
        {
            var tenants = new string[] { "tenant1", "tenant2", "tenant3", "tenant4", "tenant5" };
            var options = new EntityTableClientOptions(TestEnvironment.ConnectionString,
                $"{nameof(PersonEntity)}",
                maxItemsPerBatch: 5000,
                maxParallelTasks: 5,                
                createTableIfNotExists: true);

            //Configure entity binding in the table storage
            var entityClient = new EntityTableClient<PersonEntity>(options, config =>
            {
                config
                .SetPartitionKey(p => p.TenantId)
                .SetPrimaryKeyProp(p => p.PersonId)
                .AddTag(p => p.Created)
                .AddTag(p => p.LastName)
                .AddTag(p => p.Distance)
                .AddTag(p => p.Enabled)

                .AddComputedProp("_IsInFrance", p => p.Address.State == "France")
                .AddComputedProp("_MoreThanOneAddress", p => p.OtherAddress.Count > 1)
                .AddComputedProp("_CreatedNext6Month", p => p.Created > DateTimeOffset.UtcNow.AddMonths(-6))
                .AddComputedProp("_FirstLastName3Chars", p => p.LastName.ToLower()[..3])

                .AddTag("_FirstLastName3Chars");
            });

            var faker = Fakers.CreateFakePerson(tenants);

            Console.Write($"Generate faked {ENTITY_COUNT} entities...");
            var entities = faker.Generate(ENTITY_COUNT);
            Console.WriteLine("OK");

            var counters = new PerfCounters(nameof(EntityTableClient<PersonEntity>));
            Console.Write($"Insert {ENTITY_COUNT} entities...");

            using (var mesure = counters.Mesure($"Add many entities {ENTITY_COUNT} items"))
            {
                await entityClient.AddManyAsync(entities);
            }

            var entity = faker.Generate(1).FirstOrDefault();
            using (var mesure = counters.Mesure($"Add one entity"))
            {
                await entityClient.AddAsync(entity);
            }
            using (var mesure = counters.Mesure($"Add or replace one entity"))
            {
                await entityClient.AddOrReplaceAsync(entity);
            }

            Console.WriteLine($"Querying entities ...");
            var person = entities.First();

            using (var mesure = counters.Mesure("1. Get By Id"))
            {
                _ = await entityClient.GetByIdAsync(person.TenantId, person.PersonId);
            }

            using (var mesure = counters.Mesure("2. Get By LastName"))
            {
                await foreach (var _ in entityClient.GetAsync(
                        person.TenantId,
                        w => w.Where(p => p.LastName).Equal(person.LastName)))
                {
                    Console.WriteLine($"{mesure.Name} iterate { _.Count()}");
                }
            }

            using (var mesure = counters.Mesure("3. Get By LastName (indexed tag)"))
            {
                await foreach (var _ in entityClient.GetByTagAsync(
                        person.TenantId,
                        p => p.LastName,
                        person.LastName))
                {
                    Console.WriteLine($"{mesure.Name} iterate { _.Count()}");
                }
            }

            using (var mesure = counters.Mesure("4. Get LastName start with 'arm'"))
            {
                await foreach (var _ in entityClient.GetAsync(
                        person.TenantId,
                        w => w.Where("_FirstLastName3Chars").Equal("arm")))
                {
                    Console.WriteLine($"{mesure.Name} iterate { _.Count()}");
                }
            }

            using (var mesure = counters.Mesure("5. Get by LastName start with 'arm' (indexed tag)"))
            {
                await foreach (var _ in entityClient.GetByTagAsync(
                      person.TenantId,
                     "_FirstLastName3Chars", "arm"))
                {
                    Console.WriteLine($"{mesure.Name} iterate {_.Count()}");
                }
            }

            Console.WriteLine("====================================");
            foreach (var counter in counters.Get().OrderBy(c => c.Key))
            {
                WriteLineDuration($"{counter.Key} ", counter.Value);
            }
            Console.WriteLine("Finished");
            Console.ReadLine();
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