using Azure.Data.Tables;
using Azure.EntityServices.Queries;
using Azure.EntityServices.Tables;
using Azure.EntityServices.Tables.Extensions;
using Common.Samples;
using Common.Samples.Models;
using Common.Samples.Tools.Fakes;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace Azure.EntityServices.Table.Tests
{
    [TestClass]
    public class EntityTableClientTests
    {
        private readonly Action<EntityTableClientOptions> _commonOptions;

        public EntityTableClientTests()
        {
            _commonOptions = (EntityTableClientOptions options) =>
            {
                options.CreateTableIfNotExists = true;
                options.TableName = $"{nameof(EntityTableClientTests)}{Guid.NewGuid():N}";
                options.HandleTagMutation = true;
            };
        }

        [TestMethod]
        public async Task Should_InsertOrReplace_Entity()
        {
            var persons = Fakers.CreateFakePerson().Generate(1);
            var person = persons.First();

            var entityTable = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString)
                .Configure(options => _commonOptions(options), c =>
            {
                c.
                 SetPartitionKey(p => p.TenantId)
                .SetRowKeyProp(p => p.PersonId)
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

            var entityTable = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString)
                .Configure(options => _commonOptions(options), c =>
            {
                c.
                 SetPartitionKey(p => p.TenantId)
                .SetRowKeyProp(p => p.PersonId)
                .AddTag(p => p.LastName)
                .AddTag(p => p.Created);
            });
            await entityTable.AddOrReplaceAsync(person);

            var oldLastName = person.LastName;
            person.LastName += "_updated";
            await entityTable.AddOrReplaceAsync(person);

            var created = await entityTable.GetByIdAsync(person.TenantId, person.PersonId);

            created.LastName.Should().BeEquivalentTo(person.LastName);

            var tagResult = await entityTable.GetAsync(f => f.WhereTag(p => p.LastName)
            .Equal(person.LastName)).ToListAsync();

            tagResult.Count.Should().Be(1);
            tagResult.First().Should().BeEquivalentTo(person);

            var anyResult = await entityTable
                .GetAsync(f => f.WhereTag(p => p.LastName)
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
            var entityTable = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString)
                .Configure(
                options => _commonOptions(options),
                config =>
             {
                 config.
                  SetPartitionKey(p => p.TenantId)
                 .SetRowKeyProp(p => p.PersonId)
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

            var entityTable = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString).Configure(options => _commonOptions(options), c =>
            {
                c.
                 SetPartitionKey(p => p.TenantId)
                .SetRowKeyProp(p => p.PersonId)
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

            var entityTable = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString).Configure(options => _commonOptions(options), c =>
            {
                c.
                SetPartitionKey(p => p.TenantId)
                .SetRowKeyProp(p => p.PersonId)
                .AddTag(p => p.LastName)
                .AddTag(p => p.Created);
            });
            try
            {
                await entityTable.AddManyAsync(persons);

                var person = persons.First();
                //get all entities both primary and projected
                await foreach (var resultPage in entityTable.GetAsync(
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
            catch { throw; }
            finally
            {
                await entityTable.DropTableAsync();
            }
        }

        [TestMethod]
        public async Task Should_Get_By_Indexed_Tag_Without_Given_Partition_Key()
        {
            var persons = Fakers.CreateFakePerson().Generate(10);

            var entityTable = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString).Configure(options => _commonOptions(options), c =>
            {
                c.
                SetPartitionKey(p => p.TenantId)
                .SetRowKeyProp(p => p.PersonId)
                .AddTag(p => p.LastName)
                .AddTag(p => p.Created);
            });
            try
            {
                await entityTable.AddManyAsync(persons);

                var person = persons.First();
                //get all entities both primary and projected
                await foreach (var resultPage in entityTable.GetAsync(
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
            catch { throw; }
            finally
            {
                await entityTable.DropTableAsync();
            }
        }

        [TestMethod]
        public async Task Should_Set_Primary_Key_On_InsertOrUpdate()
        {
            var person = Fakers.CreateFakePerson().Generate();
            var tableEntity = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString).Configure(options => _commonOptions(options), c =>
            {
                c.SetPartitionKey(p => p.TenantId);
                c.SetRowKeyProp(p => p.PersonId);
            });
            try
            {
                await tableEntity.AddOrReplaceAsync(person);
                var created = await tableEntity.GetByIdAsync(person.TenantId, person.PersonId);
                created.Should().BeEquivalentTo(person);
            }
            catch { throw; }
            finally
            {
                await tableEntity.DropTableAsync();
            }
        }

        [TestMethod]
        public async Task Should_Get_Indexed_Tag_After_InsertOrUpdate()
        {
            var person = Fakers.CreateFakePerson().Generate();
            var tableEntity = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString).Configure(options => _commonOptions(options), c =>
            {
                c.SetPartitionKey(p => p.TenantId);
                c.SetRowKeyProp(p => p.PersonId);
                c.AddTag(p => p.LastName);
            });
            try
            {
                await tableEntity.AddOrReplaceAsync(person);
                await foreach (var resultPage in tableEntity.GetAsync(
                       filter => filter
                    .WhereTag(p => p.LastName)
                    .Equal(person.LastName).AndPartitionKey().Equal(person.TenantId)))
                {
                    resultPage.Count().Should().Be(1);
                    resultPage.First().Should().BeEquivalentTo(person);
                }
            }
            catch { throw; }
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
            var tableEntity = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString).Configure(options => _commonOptions(options), c =>
            {
                c.SetPartitionKey(p => p.TenantId);
                c.SetRowKeyProp(p => p.PersonId);
                c.AddComputedProp("_FirstLastName3Chars", p => First3Char(p.LastName));
            });
            try
            {
                await tableEntity.AddOrReplaceAsync(person);
                var created = await tableEntity.GetByIdAsync(person.TenantId, person.PersonId);
                First3Char(created.LastName).Should().Be(First3Char(person.LastName));
            }
            catch { throw; }
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
            var tableEntity = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString).Configure(options => _commonOptions(options), c =>
            {
                c.SetPartitionKey(p => p.TenantId);
                c.SetRowKeyProp(p => p.PersonId);
                c.AddComputedProp("_FirstLastName3Chars", p => First3Char(p.LastName));
                c.AddTag("_FirstLastName3Chars");
            });
            try
            {
                await tableEntity.AddOrReplaceAsync(person);
                await foreach (var resultPage in tableEntity.GetAsync(
                     filter => filter
                    .WhereTag("_FirstLastName3Chars")
                    .Equal(First3Char(person.LastName))
                    .AndPartitionKey()
                    .Equal(person.TenantId)))
                {
                    First3Char(resultPage.FirstOrDefault()?.LastName ?? "").Should().Be(First3Char(person.LastName));
                }
            }
            catch { throw; }
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

            var tableEntity = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString).Configure(options => _commonOptions(options), c =>
            {
                c.SetPartitionKey(p => p.TenantId);
                c.SetRowKeyProp(p => p.PersonId);
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
                await foreach (var resultPage in tableEntity.GetAsync(
                    filter => filter
                    .WhereTag("_FirstLastName3Chars")
                    .Equal(First3Char(person.LastName))
                    .AndPartitionKey()
                    .Equal(person.TenantId)))
                {
                    resultPage.Should().BeEmpty();
                }

                await foreach (var resultPage in tableEntity.GetAsync(
                    filter => filter
                     .WhereTag(p => p.LastName)
                    .Equal(person.LastName)
                    .AndPartitionKey()
                    .Equal(person.TenantId)))
                {
                    resultPage.Should().BeEmpty();
                }
            }
            catch { throw; }
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

            var tableEntity = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString).Configure(options => _commonOptions(options), c =>
            {
                c.SetPartitionKey(p => p.TenantId)
                .SetRowKeyProp(p => p.PersonId)
                .AddTag(p => p.LastName)
                .AddObserver(nameof(DummyObserver), () => observer);
            });
            try
            {
                await tableEntity.AddManyAsync(persons);

                await tableEntity.DeleteAsync(persons.Skip(1).First());

                observer.Persons.Should().HaveCount(9);
                observer.CreatedCount.Should().Be(10);
                observer.DeletedCount.Should().Be(1);
            }
            catch { throw; }
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

            IEntityTableClient<PersonEntity> tableEntity = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString).Configure(options => _commonOptions(options), config =>
            {
                config
                .SetPartitionKey(p => p.TenantId)
                .SetRowKeyProp(p => p.PersonId)
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
            catch { throw; }
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

            IEntityTableClient<PersonEntity> tableEntity = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString).Configure(options => _commonOptions(options), config =>
            {
                config
                .SetPartitionKey(p => p.TenantId)
                .SetRowKeyProp(p => p.PersonId)
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
                .IgnoreTags()
                .AndPartitionKey()
                .Equal(persons.First().TenantId)))
                {
                    pagedResult.Should().HaveCount(130);
                    pagedResult.All(person => person.LastName.EndsWith("_updated")).Should().BeTrue();
                }
            }
            catch { throw; }
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

            IEntityTableClient<PersonEntity> tableEntity = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString).Configure(options => _commonOptions(options), config =>
            {
                config
                .SetPartitionKey(p => p.TenantId)
                .SetRowKeyProp(p => p.PersonId)
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
            catch { throw; }
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

            IEntityTableClient<PersonEntity> tableEntity = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString).Configure(options => _commonOptions(options), config =>
            {
                config
                .SetPartitionKey(p => p.TenantId)
                .SetRowKeyProp(p => p.PersonId)
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
            catch { throw; }
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

            IEntityTableClient<PersonEntity> tableEntity = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString).Configure(options => _commonOptions(options), config =>
            {
                config
                .SetPartitionKey(p => p.TenantId)
                .SetRowKeyProp(p => p.PersonId)
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
            catch { throw; }
            finally
            {
                await tableEntity.DropTableAsync();
            }
        }

        [TestMethod]
        public async Task Should_Query_By_Tag_Filter_With_Equal_Extension()
        {
            var persons = Fakers.CreateFakePerson().Generate(10);

            var entityTable = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString).Configure(options => _commonOptions(options), c =>
            {
                c.
                SetPartitionKey(p => p.TenantId)
                .SetRowKeyProp(p => p.PersonId)
                .AddTag(p => p.LastName)
                .AddTag(p => p.Created);
            });
            try
            {
                await entityTable.AddManyAsync(persons);

                var person = persons.Last();

                await foreach (var resultPage in entityTable.GetAsync(
                filter => filter
                .WhereTag(p => p.Created)
                .Equal(person.Created)))
                {
                    resultPage.First().Should().BeEquivalentTo(person);
                }
            }
            catch { throw; }
            finally
            {
                await entityTable.DropTableAsync();
            }
        }

        [TestMethod]
        public async Task Should_Query_By_Tag_Filter_With_GreaterThanOrEqual()
        {
            var persons = Fakers.CreateFakePerson().Generate(10);

            var entityTable = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString).Configure(options => _commonOptions(options), c =>
            {
                c.
                SetPartitionKey(p => p.TenantId)
                .SetRowKeyProp(p => p.PersonId)
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

                await foreach (var resultPage in entityTable.GetAsync(filter => filter.WhereTag("Created").GreaterThanOrEqual(oldestDate)))
                {
                    resultPage.Should().BeEquivalentTo(persons.Where(p => p != olderPerson));
                }
            }
            catch { throw; }
            finally
            {
                await entityTable.DropTableAsync();
            }
        }

        [TestMethod]
        public async Task Should_Query_By_Tag_Filter_With_GreaterThan()
        {
            var persons = Fakers.CreateFakePerson().Generate(10);

            var entityTable = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString).Configure(options => _commonOptions(options), c =>
            {
                c.
                SetPartitionKey(p => p.TenantId)
                .SetRowKeyProp(p => p.PersonId)
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

                await foreach (var resultPage in entityTable.GetAsync(
                    filter => filter
                    .WhereTag("Created").GreaterThan(oldestDate)))
                {
                    resultPage.Select(p => p.Created).All(p => p.Value > oldestDate).Should().BeTrue();
                    resultPage.Should().NotContain(olderPerson);
                }
            }
            catch { throw; }
            finally
            {
                await entityTable.DropTableAsync();
            }
        }

        [TestMethod]
        public async Task Should_Query_By_Tag_Filter_With_LessThan()
        {
            var persons = Fakers.CreateFakePerson().Generate(10);

            var entityTable = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString).Configure(options => _commonOptions(options), c =>
            {
                c.
                SetPartitionKey(p => p.TenantId)
                .SetRowKeyProp(p => p.PersonId)
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

                await foreach (var resultPage in entityTable.GetAsync(
                    filter => filter
                    .WhereTag(p => p.Created)
                    .LessThan(latestDate)))
                {
                    resultPage.Select(p => p.Created).All(p => p.Value < latestDate).Should().BeTrue();
                    resultPage.Should().NotContain(lastestPerson);
                }
            }
            catch { throw; }
            finally
            {
                await entityTable.DropTableAsync();
            }
        }

        [TestMethod]
        public async Task Should_Query_By_Tag_Filter_With_LessThanOrEqual()
        {
            var persons = Fakers.CreateFakePerson().Generate(10);

            var entityTable = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString).Configure(options => _commonOptions(options), c =>
            {
                c.
                SetPartitionKey(p => p.TenantId)
                .SetRowKeyProp(p => p.PersonId)
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
                await foreach (var resultPage in entityTable.GetAsync(

                    filter => filter
                    .WhereTag("Created")
                    .LessThanOrEqual(latestDate)))
                {
                    resultPage.Should().BeEquivalentTo(persons.Where(p => p != lastestPerson));
                }
            }
            catch { throw; }
            finally
            {
                await entityTable.DropTableAsync();
            }
        }

        [TestMethod]
        public async Task Should_Query_By_Tag_Filter_With_Between_Extension()
        {
            var persons = Fakers.CreateFakePerson().Generate(10);

            var entityTable = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString).Configure(options => _commonOptions(options), c =>
            {
                c.
                SetPartitionKey(p => p.TenantId)
                .SetRowKeyProp(p => p.PersonId)
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
                await foreach (var resultPage in entityTable.GetAsync(
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
            catch { throw; }
            finally
            {
                await entityTable.DropTableAsync();
            }
        }

        [TestMethod]
        public async Task Should_Combine_Query_With_Mixed_Tag_And_Prop_Filters()
        {
            var persons = Fakers.CreateFakePerson().Generate(10);

            var entityTable = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString).Configure(options => _commonOptions(options), c =>
            {
                c.
                SetPartitionKey(p => p.TenantId)
                .SetRowKeyProp(p => p.PersonId)
                .AddTag(p => p.LastName)
                .AddTag(p => p.Created);
            });
            try
            {
                persons.Last().Created = DateTimeOffset.UtcNow;
                persons.Last().Altitude = -100;

                await entityTable.AddManyAsync(persons);
                //Query by Created Tag
                await foreach (var resultPage in entityTable.GetAsync(
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
            catch { throw; }
            finally
            {
                await entityTable.DropTableAsync();
            }
        }

        [TestMethod]
        public async Task Should_Query_By_Tag_Filter_With_Nullable_Values()
        {
            var persons = Fakers.CreateFakePerson().Generate(10);

            var entityTable = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString).Configure(options => _commonOptions(options), c =>
            {
                c.
                SetPartitionKey(p => p.TenantId)
                .SetRowKeyProp(p => p.PersonId)
                .AddTag(p => p.LastName)
                .AddTag(p => p.Created);
            });
            try
            {
                var person = persons.Last();
                person.Created = null;

                await entityTable.AddManyAsync(persons);

                await foreach (var resultPage in entityTable.GetAsync(
                filter => filter
                .WhereTag(p => p.Created)
                .Equal(null)))
                {
                    resultPage.First().Should().BeEquivalentTo(person);
                }
            }
            catch { throw; }
            finally
            {
                await entityTable.DropTableAsync();
            }
        }

        [TestMethod]
        public async Task Should_Query_By_Tag_Filter_With_Empty_Values()
        {
            var persons = Fakers.CreateFakePerson().Generate(1);

            var entityTable = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString).Configure(options => _commonOptions(options), c =>
            {
                c.
                SetPartitionKey(p => p.TenantId)
                .SetRowKeyProp(p => p.PersonId)
                .AddTag(p => p.LastName)
                .AddTag(p => p.Created);
            });
            try
            {
                var person = persons.Last();
                person.LastName = string.Empty;

                await entityTable.AddManyAsync(persons);
                await foreach (var resultPage in entityTable.GetAsync(
                filter => filter
                .WhereTag(p => p.LastName)
                .Equal("")))
                {
                    resultPage.First().Should().BeEquivalentTo(person);
                }
            }
            catch { throw; }
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

            var entityTable = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString).Configure(options => _commonOptions(options), c =>
            {
                c
                .SetPartitionKey(p => p.TenantId)
                .SetRowKeyProp(p => p.PersonId)
                .AddTag(p => p.LastName)
                .AddTag(p => p.Created);
            });
            try
            {
                await entityTable.AddOrReplaceAsync(person);
                var added = await entityTable.GetByIdAsync(person.TenantId, person.PersonId);
                added.Should().BeEquivalentTo(person);
            }
            catch { throw; }
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

            var entityTable = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString).Configure(options => _commonOptions(options), c =>
            {
                c
                .SetPartitionKey(p => p.TenantId)
                .SetRowKeyProp(p => p.PersonId)
                .AddTag(p => p.LastName)
                .AddTag(p => p.Created);
            });
            try
            {
                await entityTable.AddOrReplaceAsync(person);
                var added = await entityTable.GetByIdAsync(person.TenantId, person.PersonId);
                added.Should().BeEquivalentTo(person);
            }
            catch { throw; }
            finally
            {
                await entityTable.DropTableAsync();
            }
        }

        [TestMethod]
        public async Task Should_Escape_Not_Supported_Char_For_PartitionKeys()
        {
            var person = Fakers.CreateFakePerson().Generate(1).FirstOrDefault();

            person.TenantId = "/\\#?Tenant123!\n\t\r\0\u0007\u009f4";
            person.Created = null;
            person.LocalCreated = null;

            var entityTable = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString).Configure(options => _commonOptions(options), c =>
            {
                c
                .SetPartitionKey(p => p.TenantId)
                .SetRowKeyProp(p => p.PersonId)
                .AddTag(p => p.LastName)
                .AddTag(p => p.Created);
            });
            try
            {
                await entityTable.AddOrReplaceAsync(person);
                var added = await entityTable.GetByIdAsync(person.TenantId, person.PersonId);
                added.Should().BeEquivalentTo(person);

                var direct = await entityTable.GetAsync(f => f.WherePartitionKey().Equal("*Tenant123!*4")).FirstAsync();
                direct.Should().BeEquivalentTo(person);
            }
            catch { throw; }
            finally
            {
                await entityTable.DropTableAsync();
            }
        }

        [TestMethod]
        public async Task Should_Escape_Not_Supported_Char_For_RowKeys()
        {
            var person = Fakers.CreateFakePerson().Generate(1).FirstOrDefault();

            person.LastName = "/\\#?Person123!\n\t\r\0\u0007\u009f4";
            person.Created = null;
            person.LocalCreated = null;

            var entityTable = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString).Configure(options => _commonOptions(options), c =>
            {
                c
                .SetPartitionKey(p => p.TenantId)
                .SetRowKeyProp(p => p.LastName)
                .AddTag(p => p.LastName)
                .AddTag(p => p.Created);
            });
            try
            {
                await entityTable.AddOrReplaceAsync(person);
                var added = await entityTable.GetByIdAsync(person.TenantId, person.LastName);
                added.Should().BeEquivalentTo(person);
                var mainRow = await entityTable.GetAsync(f => f.WhereRowKey().Equal("*Person123!*4")).FirstOrDefaultAsync();
                mainRow.Should().BeEquivalentTo(person);

                var createdRow = await entityTable.GetAsync(f => f.WhereTag("LastName").Equal(person.LastName)).FirstOrDefaultAsync();
                createdRow.Should().BeEquivalentTo(person);

                var lastNameRow = await entityTable.GetAsync(f => f.WhereTag("Created").Equal(person.Created)).FirstOrDefaultAsync();
                lastNameRow.Should().BeEquivalentTo(person);
            }
            catch { throw; }
            finally
            {
                await entityTable.DropTableAsync();
            }
        }

        [TestMethod]
        public async Task Should_Serialize_Enum_As_String()
        {
            var person = Fakers.CreateFakePerson().Generate(1).FirstOrDefault();
            var options = new EntityTableClientOptions() { };
            _commonOptions.Invoke(options);
            options.ConfigureSerializerWithStringEnumConverter();

            var genericClient = EntityTableClient.Create<TableEntity>(TestEnvironment.ConnectionString)
                .Configure(options, c =>
             {
                 c
                 .SetPartitionKey(p => p.PartitionKey)
                 .SetRowKeyProp(p => p.RowKey);
             });

            var personClient = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString)
                .Configure(options,
            c =>
            {
                c
                .SetPartitionKey(p => p.TenantId)
                .SetRowKeyProp(p => p.LastName)
                .AddTag(p => p.LastName)
                .AddTag(p => p.Created);
            });
            try
            {
                await personClient.AddOrReplaceAsync(person);

                var genericEntity = await genericClient.GetByIdAsync(person.TenantId, person.LastName);
                var serializedAddressIncludingEnum = JsonSerializer.Serialize(person.Address, new JsonSerializerOptions
                {
                    Converters = {
                        new JsonStringEnumConverter()
                    }
                });
                genericEntity.GetString("Address")?.Should().Be(serializedAddressIncludingEnum);

                var updatedEntity = await personClient.GetByIdAsync(person.TenantId, person.LastName);
                updatedEntity.Address.Should().BeEquivalentTo(person.Address);
            }
            catch { throw; }
            finally
            {
                await personClient.DropTableAsync();
            }
        }

        [TestMethod]
        public async Task Should_DeSerialize_Existing_Enum_As_Integer()
        {
            var person = Fakers.CreateFakePerson().Generate(1).FirstOrDefault();
            var options = new EntityTableClientOptions() { };
            _commonOptions.Invoke(options);

            var genericClient = EntityTableClient.Create<TableEntity>(TestEnvironment.ConnectionString)
                .Configure(options, c =>
                {
                    c
                    .SetPartitionKey(p => p.PartitionKey)
                    .SetRowKeyProp(p => p.RowKey);
                });

            var personClient = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString)
                .Configure(options,
            c =>
            {
                c
                .SetPartitionKey(p => p.TenantId)
                .SetRowKeyProp(p => p.LastName)
                .AddTag(p => p.LastName)
                .AddTag(p => p.Created);
            });
            try
            {
                await personClient.AddOrReplaceAsync(person);

                var genericEntity = await genericClient.GetByIdAsync(person.TenantId, person.LastName);

                var serializedAddress = JsonSerializer.Serialize(person.Address);

                genericEntity.GetString("Address")?.Should().Be(
                    serializedAddress,
                    because: "By default, AdressType will be serialized as integer");

                // deserializer should map json integer value with related enum value
                var updatedEntity = await personClient.GetByIdAsync(person.TenantId, person.LastName);
                updatedEntity.Address.Should()
                    .BeEquivalentTo(person.Address);
            }
            catch { throw; }
            finally
            {
                await personClient.DropTableAsync();
            }
        }

        [TestMethod]
        public async Task Should_Get_Paged_Entities()
        {
            var persons = Fakers.CreateFakePerson().Generate(155);
            var options = new EntityTableClientOptions() { };
            _commonOptions.Invoke(options);

            var personClient = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString)
                .Configure(options,
            c =>
            {
                c
                .SetPartitionKey(p => p.TenantId)
                .SetRowKeyProp(p => p.LastName)
                .AddTag(p => p.LastName)
                .AddTag(p => p.Created);
            });
            try
            {
                await personClient.AddManyAsync(persons);

                EntityPage<PersonEntity> page = default;
                int entityCount = 0;

                do
                {
                    page = await personClient.GetPagedAsync(nextPageToken: page.ContinuationToken);

                    entityCount += page.Entities.Count();
                }
                while (!page.isLastPage);

                entityCount.Should().Be(155);
            }
            catch { throw; }
            finally
            {
                await personClient.DropTableAsync();
            }
        }

        //[DataRow[ available_entities, max_per_page ]
        [DataRow(10, 20)] //skip more than available entities
        [DataRow(158, 100)]
        [DataRow(1024, 999)]
        [DataRow(2012, 1000)]
        [TestMethod]
        public async Task Should_Get_Paged_Entities_With_Custom_Max_Per_Page(params int[] inputs)
        {
            int totalCount = inputs[0];
            int maxPerPage = inputs[1];

            var persons = Fakers.CreateFakePerson().Generate(totalCount);
            var options = new EntityTableClientOptions() { };
            _commonOptions.Invoke(options);

            var personClient = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString)
                .Configure(options,
            c =>
            {
                c
                .SetPartitionKey(p => p.TenantId)
                .SetRowKeyProp(p => p.LastName)
                .AddTag(p => p.LastName)
                .AddTag(p => p.Created);
            });
            try
            {
                await personClient.AddManyAsync(persons);

                EntityPage<PersonEntity> page = default;
                int entityCount = 0;

                do
                {
                    page = await personClient.GetPagedAsync(maxPerPage: maxPerPage, iteratedCount: page.IteratedCount, nextPageToken: page.ContinuationToken);

                    if (page.isLastPage)
                    {
                        page.Entities.Count().Should().BeLessThanOrEqualTo(maxPerPage);
                    }
                    else
                    {
                        page.Entities.Count().Should().Be(maxPerPage);
                    }
                    entityCount += page.Entities.Count();
                }
                while (!page.isLastPage);

                entityCount.Should().Be(totalCount);
                page.IteratedCount.Should().Be(entityCount);
            }
            catch { throw; }
            finally
            {
                await personClient.DropTableAsync();
            }
        }

        //[DataRow[ available_entities, to_skip]
        [DataRow(10, 10)] //skip more than available entities
        [DataRow(158, 10)]
        [DataRow(122, 5)]
        [DataRow(1024, 500)]
        [DataRow(2012, 1200)]
        [TestMethod]
        public async Task Should_Skip_Entities_With_AsyncEnumerableExtensions(params int[] inputs)
        {
            int totalCount = inputs[0];
            int skipCount = inputs[1];

            var persons = Fakers.CreateFakePerson().Generate(totalCount);
            var options = new EntityTableClientOptions() { };
            _commonOptions.Invoke(options);

            var personClient = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString)
                .Configure(options,
            c =>
            {
                c
                .SetPartitionKey(p => p.TenantId)
                .SetRowKeyProp(p => p.LastName)
                .AddTag(p => p.LastName)
                .AddTag(p => p.Created);
            });
            try
            {
                await personClient.AddManyAsync(persons);

                var all = await personClient.GetAsync().ToListAsync();

                var skipResult = await personClient
                    .GetAsync()
                    .SkipAsync(skipCount)
                    .ToListAsync();

                skipResult.Count.Should().Be(Math.Max(0, totalCount - skipCount));
            }
            catch
            {
                throw;
            }
            finally
            {
                await personClient.DropTableAsync();
            }
        }

        //[DataRow[ available_entities, to_take ]
        [DataRow(10, 20)] //skip more than available entities
        [DataRow(158, 20)]
        [DataRow(122, 100)]
        [DataRow(1024, 999)]
        [DataRow(2012, 1000)]
        [TestMethod]
        public async Task Should_Take_Entities_With_AsyncEnumerableExtensions(params int[] inputs)
        {
            int totalCount = inputs[0];
            int takeCount = inputs[1];

            var persons = Fakers.CreateFakePerson().Generate(totalCount);
            var options = new EntityTableClientOptions() { };
            _commonOptions.Invoke(options);

            var personClient = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString)
                .Configure(options,
            c =>
            {
                c
                .SetPartitionKey(p => p.TenantId)
                .SetRowKeyProp(p => p.LastName)
                .AddTag(p => p.LastName)
                .AddTag(p => p.Created);
            });
            try
            {
                await personClient.AddManyAsync(persons);

                var all = await personClient.GetAsync().ToListAsync();

                var takeResult = await personClient
                    .GetAsync()
                    .TakeAsync(takeCount)
                    .ToListAsync();

                takeResult.Count.Should().Be(Math.Min(totalCount, takeCount));
            }
            catch { throw; }
            finally
            {
                await personClient.DropTableAsync();
            }
        }
    }
}