using Azure.EntityServices.Queries;
using Azure.EntityServices.Table.Common.Fakes;
using Azure.EntityServices.Table.Common.Models;
using Azure.EntityServices.Tables;
using Azure.EntityServices.Tables.Extensions;
using Azure.EntityServices.Tests.Common;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Azure.EntityServices.Table.Tests
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
                .IgnoreProp(p => p.Genre)
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
        public async Task Should_Get_By_Indexed_Tag_With_Filter()
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
                await foreach (var resultPage in entityTable.GetByTagAsync(p => p.Created,
                    filter => filter
                    .Equal(person.Created)
                    .And(p => p.Rank)
                    .Equal(person.Rank)
                    .AndPartitionKey()
                    .Equal(person.TenantId)))
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
        public async Task Should_Get_By_Indexed_Tag_Without_Given_Partition_Key()
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
                await foreach (var resultPage in entityTable.GetByTagAsync(p => p.Created,
                    filter: p => p.Equal(person.Created)
                    .AndPartitionKey()
                    .Equal(person.TenantId)
                    .And(p => p.Rank)
                    .Equal(person.Rank)))
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
        public async Task Should_Get_Indexed_Tag_After_InsertOrUpdate()
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
                await foreach (var resultPage in tableEntity.GetByTagAsync(p => p.LastName, filter => filter.Equal(person.LastName).AndPartitionKey().Equal(person.TenantId)))
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
                await foreach (var resultPage in tableEntity.GetByTagAsync( "_FirstLastName3Chars",
                    filter=>filter
                    .Equal(First3Char(person.LastName))
                    .AndPartitionKey()
                    .Equal(person.TenantId)))
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
        public async Task Should_Remove_Tags_OnDelete()
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
                await foreach (var resultPage in tableEntity.GetByTagAsync("_FirstLastName3Chars",
                    filter=> filter
                    .Equal(First3Char(person.LastName))
                    .AndPartitionKey()
                    .Equal(person.TenantId)))
                {
                    resultPage.Should().BeEmpty();
                }

                await foreach (var resultPage in tableEntity.GetByTagAsync(p => p.LastName, 
                    filter => filter
                    .Equal(person.LastName)
                    .AndPartitionKey()
                    .Equal(person.TenantId)))
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
                await foreach (var pagedResult in tableEntity.GetAsync(filter=>filter
                .WherePartitionKey()
                .Equal(persons.First().TenantId)))
                {
                    pagedResult.Should().HaveCount(130);
                }
            }
            finally
            {
                await tableEntity.DropTableAsync();
            }
        }

        [TestMethod]
        public async Task Should_Query_By_Tag_Filter_With_Equal_Extension()
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

                var person = persons.Last();

                await foreach (var resultPage in entityTable.GetByTagAsync("Created", filter => filter.Equal(person.Created)))
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
        public async Task Should_Query_By_Tag_Filter_With_GreaterThanOrEqual()
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
                var oldestDate = persons.Min(p => p.Created.Value);
                var olderPerson = Fakers.CreateFakePerson().Generate(1).First();
                olderPerson.Created = oldestDate - TimeSpan.FromSeconds(1);
                persons.Add(olderPerson);
                await entityTable.AddManyAsync(persons);

                await foreach (var resultPage in entityTable.GetByTagAsync("Created", filter => filter.GreaterThanOrEqual(oldestDate)))
                {
                    resultPage.Should().BeEquivalentTo(persons.Where(p => p != olderPerson));
                }
            }
            finally
            {
                await entityTable.DropTableAsync();
            }
        }

        [TestMethod]
        public async Task Should_Query_By_Tag_Filter_With_GreaterThan()
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
                var oldestDate = persons.Min(p => p.Created.Value);
                var olderPerson = Fakers.CreateFakePerson().Generate(1).First();
                olderPerson.Created = oldestDate - TimeSpan.FromSeconds(1);
                persons.Add(olderPerson);
                await entityTable.AddManyAsync(persons);

                await foreach (var resultPage in entityTable.GetByTagAsync("Created", filter => filter.GreaterThan(oldestDate)))
                {
                    resultPage.Select(p => p.Created).All(p => p.Value > oldestDate).Should().BeTrue();
                    resultPage.Should().NotContain(olderPerson);
                }
            }
            finally
            {
                await entityTable.DropTableAsync();
            }
        }

        [TestMethod]
        public async Task Should_Query_By_Tag_Filter_With_LessThan()
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
                var latestDate = persons.Max(p => p.Created);
                var lastestPerson = Fakers.CreateFakePerson().Generate(1).First();
                lastestPerson.Created = latestDate + TimeSpan.FromSeconds(1);
                persons.Add(lastestPerson);
                await entityTable.AddManyAsync(persons);

                await foreach (var resultPage in entityTable.GetByTagAsync("Created", filter => filter.LessThan(latestDate)))
                {
                    resultPage.Select(p => p.Created).All(p => p.Value < latestDate).Should().BeTrue();
                    resultPage.Should().NotContain(lastestPerson);
                }
            }
            finally
            {
                await entityTable.DropTableAsync();
            }
        }

        [TestMethod]
        public async Task Should_Query_By_Tag_Filter_With_LessThanOrEqual()
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
                var latestDate = persons.Max(p => p.Created);
                var lastestPerson = Fakers.CreateFakePerson().Generate(1).First();
                lastestPerson.Created = latestDate + TimeSpan.FromSeconds(1);
                persons.Add(lastestPerson);
                await entityTable.AddManyAsync(persons);
                await foreach (var resultPage in entityTable.GetByTagAsync("Created", filter => filter.LessThanOrEqual(latestDate)))
                {
                    resultPage.Should().BeEquivalentTo(persons.Where(p => p != lastestPerson));
                }
            }
            finally
            {
                await entityTable.DropTableAsync();
            }
        }

        [TestMethod]
        public async Task Should_Query_By_Tag_Filter_With_Between_Extension()
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
                var latestDate = persons.Max(p => p.Created);
                var latestPerson = Fakers.CreateFakePerson().Generate(1).First();
                latestPerson.Created = latestDate + TimeSpan.FromSeconds(1);
                persons.Add(latestPerson);

                var oldestDate = persons.Min(p => p.Created.Value);
                var olderPerson = Fakers.CreateFakePerson().Generate(1).First();
                olderPerson.Created = oldestDate - TimeSpan.FromSeconds(1);
                persons.Add(olderPerson);

                await entityTable.AddManyAsync(persons);
                await foreach (var resultPage in entityTable.GetByTagAsync("Created", filter => filter.Between(oldestDate, latestDate)))
                {
                    resultPage.Should().BeEquivalentTo(persons
                        .Where(p => p.Created > olderPerson.Created
                        &&
                        p.Created < latestPerson.Created));
                }
            }
            finally
            {
                await entityTable.DropTableAsync();
            }
        }

        [TestMethod]
        public async Task Should_Combine_Query_With_Mixed_Tag_And_Prop_Filters()
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
                persons.Last().Created = DateTimeOffset.UtcNow;
                persons.Last().Altitude = -100;

                await entityTable.AddManyAsync(persons);
                //Query by Created Tag
                await foreach (var resultPage in entityTable.GetByTagAsync(p => p.Created,
                    tag =>
                        tag
                        .GreaterThanOrEqual(persons.Last().Created)
                        .And(p => p.Altitude)
                        .LessThanOrEqual(-100)

                    ))
                {
                    resultPage.Count().Should().BePositive();
                    resultPage.Select(p => p.Altitude).All(p => p <= 100).Should().BeTrue();
                }
            }
            finally
            {
                await entityTable.DropTableAsync();
            }
        }
    }
}