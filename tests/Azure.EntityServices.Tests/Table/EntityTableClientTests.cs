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
                TableName = $"{nameof(EntityTableClientTests)}{Guid.NewGuid():N}", 
                EnableIndexedTagSupport=true
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
        public async Task Should_Refresh_Tag_When_Value_Updated()
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
            await entityTable.AddOrReplaceAsync(person);

            var oldLastName = person.LastName;
            person.LastName += "_updated";
            await entityTable.AddOrReplaceAsync(person);

            var created = await entityTable.GetByIdAsync(person.TenantId, person.PersonId);

            created.LastName.Should().BeEquivalentTo(person.LastName);

            var tagResult = await entityTable.GetByTagAsync(f => f.WhereTag(p => p.LastName)
            .Equal(person.LastName)).ToListAsync();

            tagResult.Count.Should().Be(1);
            tagResult.First().Should().BeEquivalentTo(person);

            var anyResult = await entityTable
                .GetByTagAsync(f => f.WhereTag(p => p.LastName)
                .Equal(oldLastName))
                .AnyAsync();
            anyResult.Should().BeFalse();
        }

        [TestMethod]
        public async Task Should_InsertOrReplace_Entity_With_Null_values()
        {
            var persons = Fakers.CreateFakePerson().Generate(1);
            var person = persons.First();
            person.Altitude = null;
            var entityTable = new EntityTableClient<PersonEntity>(_commonOptions(), c =>
            {
                c.
                 SetPartitionKey(p => p.TenantId)
                .SetPrimaryKeyProp(p => p.PersonId)
                .AddTag(p => p.LastName)
                .AddTag(p => p.Created);
            });
            await entityTable.AddOrReplaceAsync(person);

            var created = await entityTable.GetByIdAsync(person.TenantId, person.PersonId);
            created.Should().BeEquivalentTo(person);
            created.Altitude.Should().Be(null);
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
                await foreach (var resultPage in entityTable.GetByTagAsync(
                    filter => filter
                    .WhereTag(p => p.Created)
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
                await foreach (var resultPage in entityTable.GetByTagAsync(
                    filter => filter
                    .WhereTag(p => p.Created)
                    .Equal(person.Created)
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
                await foreach (var resultPage in tableEntity.GetByTagAsync(
                       filter => filter
                    .WhereTag(p => p.LastName)
                    .Equal(person.LastName).AndPartitionKey().Equal(person.TenantId)))
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
                await foreach (var resultPage in tableEntity.GetByTagAsync(
                     filter => filter
                    .WhereTag("_FirstLastName3Chars")
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
                await foreach (var resultPage in tableEntity.GetByTagAsync(
                    filter => filter
                    .WhereTag("_FirstLastName3Chars")
                    .Equal(First3Char(person.LastName))
                    .AndPartitionKey()
                    .Equal(person.TenantId)))
                {
                    resultPage.Should().BeEmpty();
                }

                await foreach (var resultPage in tableEntity.GetByTagAsync(
                    filter => filter
                     .WhereTag(p => p.LastName)
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
           
            var tableEntity = new EntityTableClient<PersonEntity>(_commonOptions(), c =>
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
            
            IEntityTableClient<PersonEntity> tableEntity = new EntityTableClient<PersonEntity>(_commonOptions(), config =>
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
                await foreach (var pagedResult in tableEntity.GetAsync(
                filter => filter
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
        public async Task Should_Update_Many_Indexed_Entities()
        {
            var persons = Fakers.CreateFakePerson().Generate(130);

            //force entities to have same partition (tenantId)
            var partitionName = Guid.NewGuid().ToString();
            persons.ForEach(p => p.TenantId = partitionName);
           
            IEntityTableClient<PersonEntity> tableEntity = new EntityTableClient<PersonEntity>(_commonOptions(), config =>
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

                await tableEntity.UpdateManyAsync(person =>
                {
                    person.LastName += "_updated";
                });

                //get all entities both primary and projected
                await foreach (var pagedResult in tableEntity.GetAsync(
                filter => filter
                .WherePartitionKey()
                .Equal(persons.First().TenantId)))
                {
                    pagedResult.Should().HaveCount(130);
                    pagedResult.All(person => person.LastName.EndsWith("_updated")).Should().BeTrue();
                }
            }
            finally
            {
                await tableEntity.DropTableAsync();
            }
        }

        [TestMethod]
        public async Task Should_Update_Many_Indexed_Entities_With_No_Existing_Entities()
        {
            var persons = Fakers.CreateFakePerson().Generate(130);

            //force entities to have same partition (tenantId)
            var partitionName = Guid.NewGuid().ToString();
            persons.ForEach(p => p.TenantId = partitionName);
          
            IEntityTableClient<PersonEntity> tableEntity = new EntityTableClient<PersonEntity>(_commonOptions(), config =>
            {
                config
                .SetPartitionKey(p => p.TenantId)
                .SetPrimaryKeyProp(p => p.PersonId)
                .AddTag(p => p.LastName)
                .AddTag(p => p.Created);
            });
            try
            {
                await tableEntity.CreateTableAsync();
                var updated = await tableEntity.UpdateManyAsync(person =>
                {
                    person.LastName += "updated";
                });
                updated.Should().Be(0);
            }
            finally
            {
                await tableEntity.DropTableAsync();
            }
        }

        [TestMethod]
        public async Task Should_Filter_Entities()
        {
            var persons = Fakers.CreateFakePerson().Generate(130);

            //force entities to have same partition (tenantId)
            var partitionName = Guid.NewGuid().ToString();
            persons.ForEach(p => p.TenantId = partitionName);
           
            IEntityTableClient<PersonEntity> tableEntity = new EntityTableClient<PersonEntity>(_commonOptions(), config =>
            {
                config
                .SetPartitionKey(p => p.TenantId)
                .SetPrimaryKeyProp(p => p.PersonId)
                .AddTag(p => p.LastName)
                .AddTag(p => p.Created);
            });
            try
            {
                var latestPerson = persons.Last();
                latestPerson.Altitude = -100;
                await tableEntity.AddManyAsync(persons);
                //get all entities both primary and projected
                await foreach (var pagedResult in tableEntity.GetAsync(
                filter => filter
                .WherePartitionKey()
                .Equal(persons.First().TenantId)
                .And(p => p.Altitude)
                .Equal(latestPerson.Altitude)))
                {
                    pagedResult.Should().HaveCount(1);
                }
            }
            finally
            {
                await tableEntity.DropTableAsync();
            }
        }

        //for now query with null values are not supported in Azure Table Storage
        //event if, it works in Storage Emulator
        //[TestMethod]
        public async Task Should_Filter_Entities_With_Nullable_Properties()
        {
            var persons = Fakers.CreateFakePerson().Generate(10);

            //force entities to have same partition (tenantId)
            var partitionName = Guid.NewGuid().ToString();
            persons.ForEach(p => p.TenantId = partitionName);
           
            IEntityTableClient<PersonEntity> tableEntity = new EntityTableClient<PersonEntity>(_commonOptions(), config =>
            {
                config
                .SetPartitionKey(p => p.TenantId)
                .SetPrimaryKeyProp(p => p.PersonId)
                .AddTag(p => p.LastName)
                .AddTag(p => p.Created);
            });
            try
            {
                var latestPerson = persons.Last();
                latestPerson.BankAmount = null;
                latestPerson.Altitude = null;
                latestPerson.Situation = null;
                latestPerson.Enabled = true;
                await tableEntity.AddManyAsync(persons);
                //get all entities both primary and projected
                await foreach (var pagedResult in tableEntity.GetAsync(
                filter => filter
                .WherePartitionKey()
                    .Equal(persons.First().TenantId)
                    .And(p => p.BankAmount)
                    .Equal(null)
                    .And(p => p.Altitude)
                    .Equal(null)
                    .And(p => p.Situation)
                    .Equal(null)
                    .And(p => p.Enabled)
                    .NotEqual(null))
                    )
                {
                    pagedResult.Should().HaveCount(1);
                    pagedResult.First().Should().BeEquivalentTo(latestPerson);
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

                await foreach (var resultPage in entityTable.GetByTagAsync(
                filter => filter
                .WhereTag(p => p.Created)
                .Equal(person.Created)))
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

                await foreach (var resultPage in entityTable.GetByTagAsync(filter => filter.WhereTag("Created").GreaterThanOrEqual(oldestDate)))
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

                await foreach (var resultPage in entityTable.GetByTagAsync(
                    filter => filter
                    .WhereTag("Created").GreaterThan(oldestDate)))
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

                await foreach (var resultPage in entityTable.GetByTagAsync(
                    filter => filter
                    .WhereTag(p => p.Created)
                    .LessThan(latestDate)))
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
                await foreach (var resultPage in entityTable.GetByTagAsync(

                    filter => filter
                    .WhereTag("Created")
                    .LessThanOrEqual(latestDate)))
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
                await foreach (var resultPage in entityTable.GetByTagAsync(
                    filter =>
                    filter
                    .WhereTag(p => p.Created)
                    .Between(oldestDate, latestDate)))
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
                await foreach (var resultPage in entityTable.GetByTagAsync(
                    filter =>
                        filter
                        .WhereTag(p => p.Created)
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

        [TestMethod]
        public async Task Should_Query_By_Tag_Filter_With_Nullable_Values()
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
                var person = persons.Last();
                person.Created = null;

                await entityTable.AddManyAsync(persons);

                await foreach (var resultPage in entityTable.GetByTagAsync(
                filter => filter
                .WhereTag(p => p.Created)
                .Equal(null)))
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
        public async Task Should_Query_By_Tag_Filter_With_Empty_Values()
        {
            var persons = Fakers.CreateFakePerson().Generate(1);

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
                var person = persons.Last();
                person.LastName = string.Empty;

                await entityTable.AddManyAsync(persons);
                await foreach (var resultPage in entityTable.GetByTagAsync(
                filter => filter
                .WhereTag(p => p.LastName)
                .Equal("")))
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
        public async Task Should_Store_Default_DateTime_Values()
        {
            var person = Fakers.CreateFakePerson().Generate(1).FirstOrDefault();

            person.Created = default;
            person.Updated = default;
            person.LocalCreated = default;
            person.LocalUpdated = default;
             
            var entityTable = new EntityTableClient<PersonEntity>(_commonOptions(), c =>
            {
                c
                .SetPartitionKey(p => p.TenantId)
                .SetPrimaryKeyProp(p => p.PersonId)
                .AddTag(p => p.LastName)
                .AddTag(p => p.Created);
            });
            try
            { 
                await entityTable.AddOrReplaceAsync(person);
                var added = await entityTable.GetByIdAsync(person.TenantId,person.PersonId);
                added.Should().BeEquivalentTo(person);
            }
            finally
            {
                await entityTable.DropTableAsync();
            }
        }
        [TestMethod]
        public async Task Should_Store_Null_DateTime_Values()
        {
            var person = Fakers.CreateFakePerson().Generate(1).FirstOrDefault();

            person.Created = null; 
            person.LocalCreated = null; 

            var entityTable = new EntityTableClient<PersonEntity>(_commonOptions(), c =>
            {
                c
                .SetPartitionKey(p => p.TenantId)
                .SetPrimaryKeyProp(p => p.PersonId)
                .AddTag(p => p.LastName)
                .AddTag(p => p.Created);
            });
            try
            {
                await entityTable.AddOrReplaceAsync(person);
                var added = await entityTable.GetByIdAsync(person.TenantId, person.PersonId);
                added.Should().BeEquivalentTo(person);
            }
            finally
            {
                await entityTable.DropTableAsync();
            }
        }

        /// <summary>
        ///The forward slash(/) character

        //The backslash(\) character

        //The number sign(#) character

        //The question mark (?) character

        //Control characters from U+0000 to U+001F, including:

        //The horizontal tab(\t) character

        //The linefeed(\n) character

        //The carriage return (\r) character

        //Control characters from U+007F to U+009F

        /// </summary>
        /// <returns></returns>
        [TestMethod]
        public async Task Should_Escape_Not_Supported_Car_In_Storage_Key()
        {
            var person = Fakers.CreateFakePerson().Generate(1).FirstOrDefault();

            person.TenantId = "#tenant?0";
            person.Created = null;
            person.LocalCreated = null;

            var entityTable = new EntityTableClient<PersonEntity>(_commonOptions(), c =>
            {
                c
                .SetPartitionKey(p => p.TenantId)
                .SetPrimaryKeyProp(p => p.PersonId)
                .AddTag(p => p.LastName)
                .AddTag(p => p.Created);
            });
            try
            {
                await entityTable.AddOrReplaceAsync(person);
                var added = await entityTable.GetByIdAsync(person.TenantId, person.PersonId);
                added.Should().BeEquivalentTo(person);
            }
            finally
            {
                await entityTable.DropTableAsync();
            }
        }
    }
}