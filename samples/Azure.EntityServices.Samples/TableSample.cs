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
            //==============Entity options and configuratin section====================================================
            //set here for your technical stuff: table name, connection, parallelization 
            var entityClient = EntityTableClient.Create<PersonEntity>(options =>
            {
                options.ConnectionString = TestEnvironment.ConnectionString;
                options.TableName = $"{nameof(PersonEntity)}";
                options.CreateTableIfNotExists = true;
                options.EnableIndexedTagSupport = true;
            }

            //set here your entity behavior dynamic fields, tags, observers
            , config =>
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
            //===============================================================================================





            var fakePersons = Fakers.CreateFakePerson(new string[] { "tenant1", "tenant2", "tenant3", "tenant4", "tenant5" });
            var onePerson = fakePersons.Generate(1).FirstOrDefault();


            Console.Write($"Generate faked {ENTITY_COUNT} entities...");
            var entities = fakePersons.Generate(ENTITY_COUNT);
            Console.WriteLine("OK");

            var counters = new PerfCounters(nameof(EntityTableClient<PersonEntity>));
            Console.Write($"Insert {ENTITY_COUNT} entities...");

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
                        .Where(p => p.LastName)
                        .Equal(onePerson.LastName) 
                        ))
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
                await foreach (var _ in entityClient.GetByTagAsync(
                    filter => filter
                    .WhereTag(p => p.LastName)
                    .Equal(onePerson.LastName)))

                {
                    count += _.Count();
                    Console.WriteLine($"{mesure.Name} { count} iterated");
                    Console.CursorTop--;
                }
                Console.WriteLine();
            } 
          
            using (var mesure = counters.Mesure("Get By dynamic prop"))
            {
                var count = 0;
                await foreach (var _ in entityClient.GetAsync(
                        filter => filter
                        .Where("_FirstLastName3Chars")
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
                await foreach (var _ in entityClient.GetByTagAsync(
                    filter => filter
                    .WhereTag("_FirstLastName3Chars")
                    .Equal("arm")))
                {
                    count += _.Count();
                    Console.WriteLine($"{mesure.Name} iterated");
                    Console.CursorTop--;
                }
                Console.WriteLine();
            }

            using (var mesure = counters.Mesure("Get paged all entities for one partition"))
            {
                var count = 0;
                string token = null;
                do
                {
                    var result = await entityClient
                        .GetPagedAsync(
                           filter => filter
                           .WherePartitionKey()
                           .Equal("tenant1"),
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
            
            Console.WriteLine("====================================");
            counters.WriteToConsole();
        }
    }
}