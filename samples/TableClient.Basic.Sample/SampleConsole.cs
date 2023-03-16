using Azure.EntityServices.Queries; 
using Azure.EntityServices.Tables;
using Common.Samples;
using Common.Samples.Models;
using Common.Samples.Tools.Fakes;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace TableClient.BasicSample
{
    public static class SampleConsole
    {
        private const int ENTITY_COUNT = 200;

        public static async Task Run()
        {
            //==============Entity options and configuratin section====================================================
            //set here for your technical stuff: table name, connection, parallelization
            var _entityClient = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString)
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
                //add computed props to store and compute dynamically additional fields of the entity
                .AddComputedProp("_IsInFrance", p => p.Address?.State == "France")
                .AddComputedProp("_FirstLastName3Chars", p => p.LastName?.ToLower()[..3]);
            });
            //===============================================================================================

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
    }
}