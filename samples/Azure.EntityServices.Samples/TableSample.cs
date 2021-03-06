using Azure.EntityServices.Queries;
using Azure.EntityServices.Samples.Diagnostics;
using Azure.EntityServices.Table.Common.Fakes;
using Azure.EntityServices.Table.Common.Models;
using Azure.EntityServices.Tables;
using Azure.EntityServices.Tests.Common;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Azure.EntityServices.Samples
{
    public static class TableSample
    {
        private const int ENTITY_COUNT = 1000;

        public static async Task Run()
        {
            //define tenant values as table partitions
            var tenants = new string[] { "tenant1", "tenant2", "tenant3", "tenant4", "tenant5" };

            //default options with up to 10 parallel transactions to be processed by the pipeline
            var options = new EntityTableClientOptions(TestEnvironment.ConnectionString,
                $"{nameof(PersonEntity)}");
            //Configure entity binding in the table storage
            //partition key as TenantId property
            //primary key as PersonId property
            //1 ignored prop, 5 indexed tags and 4 computed props
            var entityClient = new EntityTableClient<PersonEntity>(options, config =>
            {
                config
                .SetPartitionKey(p => p.TenantId)
                .SetPrimaryKeyProp(p => p.PersonId)
                .IgnoreProp(p => p.OtherAddress)
                .AddTag(p => p.Created)
                .AddTag(p => p.LastName)
                .AddTag(p => p.Distance)
                .AddTag(p => p.Enabled)

                .AddComputedProp("_IsInFrance", p => p.Address?.State == "France")
                .AddComputedProp("_MoreThanOneAddress", p => p.OtherAddress?.Count > 1)
                .AddComputedProp("_CreatedNext6Month", p => p.Created > DateTimeOffset.UtcNow.AddMonths(-6))
                .AddComputedProp("_FirstLastName3Chars", p => p.LastName?.ToLower()[..3])

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
                Console.WriteLine($"{mesure.Name}");
            }
            using (var mesure = counters.Mesure("2.1 Get by prop"))
            {
                var count = 0;
                await foreach (var _ in entityClient.GetAsync(
                       filter => filter
                        .WherePartitionKey()
                        .Equal(person.TenantId)
                        .And(p => p.LastName)
                        .Equal(person.LastName)

                        ))
                {
                    count += _.Count();
                    Console.WriteLine($"{mesure.Name} {count} iterated ");
                    Console.CursorTop--;
                }
                Console.WriteLine();
            }
       
            using (var mesure = counters.Mesure("2.1 Get by prop indexed"))
            {
                var count = 0;
                await foreach (var _ in entityClient.GetByTagAsync(
                    filter => filter
                    .WhereTag(p => p.LastName)
                    .Equal(person.LastName)
                    .AndPartitionKey()
                    .Equal(person.TenantId)))

                {
                    count += _.Count();
                    Console.WriteLine($"{mesure.Name} { count} iterated");
                    Console.CursorTop--;
                }
                Console.WriteLine();
            } 
          
            using (var mesure = counters.Mesure("3.1 Get By dynamic prop"))
            {
                var count = 0;
                await foreach (var _ in entityClient.GetAsync(
                        filter => filter
                        .Where("_FirstLastName3Chars")
                        .Equal("arm")
                        .AndPartitionKey()
                        .Equal(person.TenantId)
                        ))
                {
                    count += _.Count();
                    Console.WriteLine($"{mesure.Name}  {count} iterated");
                    Console.CursorTop--;
                }
                Console.WriteLine();
            }

            using (var mesure = counters.Mesure("3.2 Get by dynamic prop indexed"))
            {
                var count = 0;
                await foreach (var _ in entityClient.GetByTagAsync(
                    filter => filter
                    .WhereTag("_FirstLastName3Chars")
                    .Equal("arm")
                    .AndPartitionKey()
                    .Equal(person.TenantId)))
                {
                    count += _.Count();
                    Console.WriteLine($"{mesure.Name} iterated");
                    Console.CursorTop--;
                }
                Console.WriteLine();
            }

            using (var mesure = counters.Mesure("4.1 Get all partition paged"))
            {
                var count = 0;
                string token = null;
                do
                {
                    var result = await entityClient.GetPagedAsync(
                           filter => filter.WherePartitionKey().Equal("tenant1"),
                           maxPerPage: 1000,
                           nextPageToken: token);
                    count += result.Entities.Count();

                    Console.WriteLine($"{mesure.Name} {count} iterated ");
                    Console.CursorTop--;
                    token = result.ContinuationToken;
                }
                while (!string.IsNullOrEmpty(token));
                Console.WriteLine();
            }

            using (var mesure = counters.Mesure("4.2 Update all partition"))
            {
                var count = 0;
                await entityClient.UpdateManyAsync(u =>
                {
                    u.LastName += "_yes";
                    Console.WriteLine($"{mesure.Name} {count++} updated");
                    Console.CursorTop--; 
                }, filter => filter
                .WherePartitionKey()
                .Equal("tenant1")); 
            } 
            
            Console.WriteLine("====================================");
            counters.WriteToConsole();
        }
    }
}