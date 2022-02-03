using Azure.EntityServices.Queries;
using Azure.EntityServices.Tables;
using Azure.EntityServices.Tests.Common;
using Azure.EntityServices.Tests.Common.Fakes;
using Azure.EntityServices.Tests.Common.Models;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Azure.EntityServices.Tests.Table
{
    [TestClass]
    public class EntityTableClientTests
    {
        private readonly Func<EntityTableClientOptions> _commonOptions;

        public EntityTableClientTests()
        {
            _commonOptions = () => new EntityTableClientOptions()
            {
                ConnectionString = TestEnvironment.ConnectionString,
                MaxParallelTasks = 1,
                MaxItemsPerInsertion = 10,
                CreateTableIfNotExists = true,
                TableName = $"{nameof(EntityTableClientTests)}{Guid.NewGuid():N}"
            };
        }

        [TestMethod]
        public async Task Should_InsertOrReplace_Entity()
        {
            var persons = Fakers.CreateFakePerson().Generate(1);
            var person = persons.First();

            var entityTable = new EntityTableClient<PersonEntity>(_commonOptions(), c =>
            {
                c.
                 SetPartitionKey(p => p.TenantId)
                .SetPrimaryKeyProp(p => p.PersonId)
                .AddTag(p => p.LastName)
                .AddTag(p => p.Created);
            });
            await entityTable.AddOrReplaceAsync(persons.First());

            var created = await entityTable.GetByIdAsync(person.TenantId, person.PersonId);
            created.Should().BeEquivalentTo(person);
        }
        [TestMethod]
        public async Task Should_Ignore_Entity_Prop()
        {
            var persons = Fakers.CreateFakePerson().Generate(1);
            var person = persons.First();

            var entityTable = new EntityTableClient<PersonEntity>(_commonOptions(), c =>
            {
                c.
                 SetPartitionKey(p => p.TenantId)
                .SetPrimaryKeyProp(p => p.PersonId)
                .IgnoreProp(p=>p.Genre)                
                .AddTag(p => p.LastName)
                .AddTag(p => p.Created);
            });

            await entityTable.AddOrReplaceAsync(person);
            person.Genre = Genre.Female;
            var created = await entityTable.GetByIdAsync(person.TenantId, person.PersonId);
            created.Genre.Should().Be(default);
            created.Should().BeEquivalentTo(person, options => options.Excluding(e => e.Genre));

        }
        [TestMethod]
        public async Task Should_Get_By_Indexed_Prop_With_Filter()
        {
            var persons = Fakers.CreateFakePerson().Generate(10);

            var entityTable = new EntityTableClient<PersonEntity>(_commonOptions(), c =>
             {
                 c.
                 SetPartitionKey(p => p.TenantId)
                 .SetPrimaryKeyProp(p => p.PersonId)
                 .AddTag(p => p.LastName)
                 .AddTag(p => p.Created);
             });
            try
            {
                await entityTable.AddManyAsync(persons);

                var person = persons.First();
                //get all entities both primary and projected
                await foreach (var resultPage in entityTable.GetByTagAsync(person.TenantId, p => p.Created, person.Created, filter: p => p.Where(p => p.Rank).Equal(person.Rank)))
                {
                    resultPage.First().Should().BeEquivalentTo(person);
                }
            }
            finally
            {
                await entityTable.DropTableAsync();
            }
        }

        [TestMethod]
        public async Task Should_Set_Primary_Key_On_InsertOrUpdate()
        {
            var person = Fakers.CreateFakePerson().Generate();
            var tableEntity = new EntityTableClient<PersonEntity>(_commonOptions(), c =>
            {
                c.SetPartitionKey(p => p.TenantId);
                c.SetPrimaryKeyProp(p => p.PersonId);
            });
            try
            {
                await tableEntity.AddOrReplaceAsync(person);
                var created = await tableEntity.GetByIdAsync(person.TenantId, person.PersonId);
                created.Should().BeEquivalentTo(person);
            }
            finally
            {
                await tableEntity.DropTableAsync();
            }
        }

        [TestMethod]
        public async Task Should_Get_Indexed_Prop_On_InsertOrUpdate()
        {
            var person = Fakers.CreateFakePerson().Generate();
            var tableEntity = new EntityTableClient<PersonEntity>(_commonOptions(), c =>
            {
                c.SetPartitionKey(p => p.TenantId);
                c.SetPrimaryKeyProp(p => p.PersonId);
                c.AddTag(p => p.LastName);
            });
            try
            {
                await tableEntity.AddOrReplaceAsync(person);
                await foreach (var resultPage in tableEntity.GetByTagAsync(person.TenantId, p => p.LastName, person.LastName))
                {
                    resultPage.Count().Should().Be(1);
                    resultPage.First().Should().BeEquivalentTo(person);
                }
            }
            finally
            {
                await tableEntity.DropTableAsync();
            }
        }

        [TestMethod]
        public async Task Should_Set_Dynamic_Prop_On_InsertOrUpdate()
        {
            static string First3Char(string s) => s.ToLower()[..3];

            var person = Fakers.CreateFakePerson().Generate();
            var tableEntity = new EntityTableClient<PersonEntity>(_commonOptions(), c =>
            {
                c.SetPartitionKey(p => p.TenantId);
                c.SetPrimaryKeyProp(p => p.PersonId);
                c.AddComputedProp("_FirstLastName3Chars", p => First3Char(p.LastName));
            });
            try
            {
                await tableEntity.AddOrReplaceAsync(person);
                var created = await tableEntity.GetByIdAsync(person.TenantId, person.PersonId);
                First3Char(created.LastName).Should().Be(First3Char(person.LastName));
            }
            finally
            {
                await tableEntity.DropTableAsync();
            }
        }

        [TestMethod]
        public async Task Should_Set_Computed_Index_On_InsertOrUpdate()
        {
            static string First3Char(string s) => s.ToLower()[..3];
            var person = Fakers.CreateFakePerson().Generate();
            var tableEntity = new EntityTableClient<PersonEntity>(_commonOptions(), c =>
            {
                c.SetPartitionKey(p => p.TenantId);
                c.SetPrimaryKeyProp(p => p.PersonId);
                c.AddComputedProp("_FirstLastName3Chars", p => First3Char(p.LastName));
                c.AddTag("_FirstLastName3Chars");
            });
            try
            {
                await tableEntity.AddOrReplaceAsync(person);
                await foreach (var resultPage in tableEntity.GetByTagAsync(person.TenantId, "_FirstLastName3Chars", First3Char(person.LastName)))
                {
                    First3Char(resultPage.FirstOrDefault()?.LastName ?? "").Should().Be(First3Char(person.LastName));
                }
            }
            finally
            {
                await tableEntity.DropTableAsync();
            }
        }

        [TestMethod]
        public async Task Should_Remove_Indexes_OnDelete()
        {
            static string First3Char(string s) => s.ToLower()[..3];

            var person = Fakers.CreateFakePerson().Generate();

            var tableEntity = new EntityTableClient<PersonEntity>(_commonOptions(), c =>
            {
                c.SetPartitionKey(p => p.TenantId);
                c.SetPrimaryKeyProp(p => p.PersonId);
                c.AddTag("_FirstLastName3Chars");
                c.AddTag(p => p.LastName);
                c.AddComputedProp("_FirstLastName3Chars", p => First3Char(p.LastName));
            });
            try
            {
                await tableEntity.AddOrReplaceAsync(person);
                var created = await tableEntity.GetByIdAsync(person.TenantId, person.PersonId);
                await tableEntity.DeleteAsync(created);

                (await tableEntity.GetByIdAsync(person.TenantId, person.PersonId)).Should().BeNull();
                await foreach (var resultPage in tableEntity.GetByTagAsync(person.TenantId, "_FirstLastName3Chars", First3Char(person.LastName)))
                {
                    resultPage.Should().BeEmpty();
                }

                await foreach (var resultPage in tableEntity.GetByTagAsync(person.TenantId, p => p.LastName, person.LastName))
                {
                    resultPage.Should().BeEmpty();
                }
            }
            finally
            {
                await tableEntity.DropTableAsync();
            }
        }

        [TestMethod]
        public async Task Should_Observe_Entity_Table_Updates()
        {
            var persons = Fakers.CreateFakePerson().Generate(10);
            var observer = new DummyObserver();
            var options = new EntityTableClientOptions()
            {
                ConnectionString = TestEnvironment.ConnectionString,
                MaxParallelTasks = 10,
                MaxItemsPerInsertion = 1000,
                TableName = $"{nameof(EntityTableClientTests)}{Guid.NewGuid():N}",
                CreateTableIfNotExists = true,
            };
            var tableEntity = new EntityTableClient<PersonEntity>(options, c =>
            {
                c.SetPartitionKey(p => p.TenantId)
                .SetPrimaryKeyProp(p => p.PersonId)
                .AddTag(p => p.LastName)
                .AddObserver(nameof(DummyObserver), observer);
            });
            try
            {
                await tableEntity.AddManyAsync(persons);

                await tableEntity.DeleteAsync(persons.Skip(1).First());

                observer.Persons.Should().HaveCount(9);
                observer.CreatedCount.Should().Be(10);
                observer.DeletedCount.Should().Be(1);
            }
            finally
            {
                await tableEntity.DropTableAsync();
            }
        }

        [TestMethod]
        public async Task Should_Insert_Many_Indexed_Entities()
        {
            var persons = Fakers.CreateFakePerson().Generate(130);

            //force entities to have same partition (tenantId)
            var partitionName = Guid.NewGuid().ToString();
            persons.ForEach(p => p.TenantId = partitionName);
            var options = new EntityTableClientOptions()
            {
                ConnectionString = TestEnvironment.ConnectionString,
                MaxParallelTasks = 10,
                MaxItemsPerInsertion = 1000,
                TableName = $"{nameof(EntityTableClientTests)}{Guid.NewGuid():N}",
                CreateTableIfNotExists = true,
            };
            IEntityTableClient<PersonEntity> tableEntity = new EntityTableClient<PersonEntity>(options, config =>
            {
                config
                .SetPartitionKey(p => p.TenantId)
                .SetPrimaryKeyProp(p => p.PersonId)
                .AddTag(p => p.LastName)
                .AddTag(p => p.Created);
            });
            try
            {
                await tableEntity.AddManyAsync(persons);

                //get all entities both primary and projected
                await foreach (var pagedResult in tableEntity.GetAsync(persons.First().TenantId))
                {
                    pagedResult.Should().HaveCount(130);
                }
            }
            finally
            {
                await tableEntity.DropTableAsync();
            }
        }
    }
}