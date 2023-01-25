using Azure.EntityServices.Queries;
using Azure.EntityServices.Table.Common.Fakes;
using Azure.EntityServices.Tables;
using Azure.EntityServices.Tests.Common;
using Common.Samples.Models;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace TableClient.Basic.Sample
{
    public static class TableSample
    {
        private const int ENTITY_COUNT = 200;

        public static async Task Run()
        {
            //==============Entity options and configuratin section====================================================
            //set here for your technical stuff: table name, connection, parallelization
            var entityClient = EntityTableClient.Create<PersonEntity>(options =>
            {
                options.ConnectionString = TestEnvironment.ConnectionString;
                options.TableName = $"{nameof(PersonEntity)}";
                options.CreateTableIfNotExists = true;
            }

            //set here your entity behavior dynamic fields, tags, observers
            , config =>
            {
                config
                .SetPartitionKey(p => p.TenantId)
                .SetRowKeyProp(p => p.PersonId)
                .IgnoreProp(p => p.OtherAddress)

                //add computed props to store and compute dynamically additional fields of the entity
                .AddComputedProp("_IsInFrance", p => p.Address?.State == "France");
            });
            //===============================================================================================

            var fakePersons = Fakers.CreateFakePerson(new string[] { "tenant1", "tenant2", "tenant3", "tenant4", "tenant5" });
            var onePerson = fakePersons.Generate(1).FirstOrDefault();

            Console.Write($"Generate faked {ENTITY_COUNT} entities...");
            var entities = fakePersons.Generate(ENTITY_COUNT);
            Console.WriteLine("OK");

            Console.Write($"Adding entities...");

            await entityClient.AddAsync(onePerson);

            await entityClient.AddOrReplaceAsync(onePerson);

            await entityClient.AddManyAsync(entities);

            await entityClient.AddOrReplaceManyAsync(entities);

            Console.WriteLine($"Querying entities ...");

            _ = await entityClient.GetByIdAsync(onePerson.TenantId, onePerson.PersonId);

            var count = 0;
            await foreach (var _ in entityClient.GetAsync(
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
            await foreach (var _ in entityClient.GetAsync(
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
            await foreach (var _ in entityClient.GetAsync(
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
            await foreach (var _ in entityClient.GetAsync(
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
           

            Console.WriteLine("====================================");
        }
    }
}