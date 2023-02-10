using Azure.EntityServices.Queries; 
using Azure.EntityServices.Tables; 
using System;
using System.Linq;
using System.Threading.Tasks;
using Common.Samples.Models;
using Common.Samples.Diagnostics;
using Common.Samples.Tools;
using Common.Samples;
using Common.Samples.Tools.Fakes;

namespace TableClient.PerformanceSample
{
    public static class SampleConsole
    {
        private const int ENTITY_COUNT = 200;

        public static async Task Run()
        {
            //==============Entity options and configuratin section====================================================
            //set here for your technical stuff: table name, connection, parallelization
            var entityClient = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString)
            .Configure(options =>
            { 
                options.TableName = $"{nameof(PersonEntity)}";
                options.CreateTableIfNotExists = true;
            }

            //set here your entity behavior dynamic fields, tags, observers
            , config =>
            {
                config
                .SetPartitionKey(entity => entity.TenantId)
                .SetRowKeyProp(entity => entity.PersonId)
                .IgnoreProp(entity => entity.OtherAddress)

                //add tag to generate indexed and sorted entities through rowKey
                .AddTag(entity => entity.Created)
                .AddTag(entity => entity.LastName)
                .AddTag(entity => entity.Distance)
                .AddTag(entity => entity.Enabled)

                //add computed props to store and compute dynamically additional fields of the entity
                .AddComputedProp("_IsInFrance", entity => entity.Address?.State == "France")
                .AddComputedProp("_MoreThanOneAddress", entity => entity.OtherAddress?.Count > 1)
                .AddComputedProp("_CreatedNext6Month", entity => entity.Created > DateTimeOffset.UtcNow.AddMonths(-6))
                .AddComputedProp("_FirstLastName3Chars", entity => entity.LastName?.ToLower()[..3])

                //computed props could also be tagged 
                .AddTag("_FirstLastName3Chars")

                //add an entity oberver to track entity changes and apply any action (projection, logging, etc.)
                .AddObserver("EntityLoggerObserver",()=> new EntityLoggerObserver<PersonEntity>());
            });
            //===============================================================================================

            var fakePersons = Fakers.CreateFakePerson(new string[] { "tenant1", "tenant2", "tenant3", "tenant4", "tenant5" });
            var onePerson = fakePersons.Generate(1).FirstOrDefault();

            Console.Write($"Generate faked {ENTITY_COUNT} entities...");
            var entities = fakePersons.Generate(ENTITY_COUNT);
            Console.WriteLine("OK");

            var counters = new PerfCounters(nameof(EntityTableClient<PersonEntity>));

            using (var mesure = counters.Mesure($"Add many entities {ENTITY_COUNT} items"))
            {
                await entityClient.AddManyAsync(entities);
            }

            using (var mesure = counters.Mesure($"Add or replace many entities {ENTITY_COUNT} items"))
            {
                await entityClient.AddOrReplaceManyAsync(entities);
            }

            using (var mesure = counters.Mesure($"Add one entity"))
            {
                await entityClient.AddAsync(onePerson);
            }

            using (var mesure = counters.Mesure($"Add or replace one entity"))
            {
                await entityClient.AddOrReplaceAsync(onePerson);
            }

            Console.WriteLine($"Querying entities ...");

            using (var mesure = counters.Mesure("Get By Id"))
            {
                _ = await entityClient.GetByIdAsync(onePerson.TenantId, onePerson.PersonId);
                Console.WriteLine($"{mesure.Name}");
            }

            using (var mesure = counters.Mesure("Get with filter "))
            {
                var count = 0;
                await foreach (var _ in entityClient.GetAsync(
                       filter => filter
                        .Where(entity => entity.LastName)
                        .Equal(onePerson.LastName)
                        .AndPartitionKey()
                        .Equal("tenant1"))
                        )
                {
                    count += _.Count();
                    Console.WriteLine($"{mesure.Name} {count} iterated ");
                    Console.CursorTop--;
                }
                Console.WriteLine();
            }

            using (var mesure = counters.Mesure("Get with filter indexed"))
            {
                var count = 0;
                await foreach (var _ in entityClient.GetAsync(
                    filter => filter
                    .WhereTag(entity => entity.LastName)
                    .Equal(onePerson.LastName)
                    .AndPartitionKey()
                    .Equal("tenant1"))
                    )

                {
                    count += _.Count();
                    Console.WriteLine($"{mesure.Name} {count} iterated");
                    Console.CursorTop--;
                }
                Console.WriteLine();
            }

            using (var mesure = counters.Mesure("Get By dynamic prop"))
            {
                var count = 0;
                await foreach (var _ in entityClient.GetAsync(
                        filter => filter
                        .WherePartitionKey()
                        .Equal("tenant1")
                        .And("_FirstLastName3Chars")
                        .Equal("arm")))
                {
                    count += _.Count();
                    Console.WriteLine($"{mesure.Name}  {count} iterated");
                    Console.CursorTop--;
                }
                Console.WriteLine();
            }

            using (var mesure = counters.Mesure("Get by dynamic prop indexed"))
            {
                var count = 0;
                await foreach (var _ in entityClient.GetAsync(
                    filter => filter
                    .WhereTag("_FirstLastName3Chars")
                    .Equal("arm")
                    .AndPartitionKey()
                    .Equal("tenant1")))
                {
                    count += _.Count();
                    Console.WriteLine($"{mesure.Name} {count} iterated");
                    Console.CursorTop--;
                }
                Console.WriteLine();
            }
            Console.WriteLine("====================================");
            counters.WriteToConsole();
        }
    }
}