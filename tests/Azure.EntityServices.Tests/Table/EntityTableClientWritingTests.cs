using Azure.Data.Tables;
using Azure.EntityServices.Queries;
using Azure.EntityServices.Tables;
using Azure.EntityServices.Tables.Extensions;
using Azure.EntityServices.Tests.Table;
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
    public class EntityTableClientWritingTests
    {
        private readonly Action<EntityTableClientOptions> _defaultOptions;

        public EntityTableClientWritingTests()
        {
            _defaultOptions = EntityTableClientCommon.DefaultOptions(nameof(EntityTableClientWritingTests));
        }

        [TestMethod]
        public async Task Should_InsertOrReplace_Entity()
        {
            var persons = Fakers.CreateFakePerson().Generate(1);
            var person = persons.First();

            var entityTable = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString)
                .Configure(options => _defaultOptions(options), c =>
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
        public async Task Should_Merge_Entity()
        {
            var persons = Fakers.CreateFakePerson().Generate(1);
            var person = persons.First();

            var entityTable = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString)
                .Configure(options => _defaultOptions(options), c =>
                {
                    c.
                     SetPartitionKey(p => p.TenantId)
                    .SetRowKeyProp(p => p.PersonId)
                    .AddTag(p => p.LastName)
                    .AddTag(p => p.Created);
                });
            await entityTable.AddOrMergeAsync(person);

            var created = await entityTable.GetByIdAsync(person.TenantId, person.PersonId);

            person.Enabled = !person.Enabled;

            await entityTable.MergeAsync(person);

            var merged = await entityTable.GetByIdAsync(person.TenantId, person.PersonId);

            created.Should().BeEquivalentTo(merged, options => options.Excluding(p => p.Enabled));
        }

        [TestMethod]
        public async Task Should_Partially_Merge_Entity()
        {
            var persons = Fakers.CreateFakePerson().Generate(1);
            var person = persons.First();

            var entityTable = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString)
                .Configure(options => _defaultOptions(options), c =>
                {
                    c.
                     SetPartitionKey(p => p.TenantId)
                    .SetRowKeyProp(p => p.PersonId)
                    .AddTag(p => p.LastName)
                    .AddTag(p => p.Created);
                });
            await entityTable.AddOrMergeAsync(person);

            var created = await entityTable.GetByIdAsync(person.TenantId, person.PersonId);

            var personToMerge = new PersonEntity()
            {
                PersonId = person.PersonId,
                TenantId = person.TenantId,
                FirstName = "updatedFirstName",
            };

            await entityTable.MergeAsync(personToMerge);

            var merged = await entityTable.GetByIdAsync(person.TenantId, person.PersonId);

            merged.Should().NotBeNull();
            merged.FirstName.Should().Be("updatedFirstName");
        }

        [TestMethod]
        public async Task Should_Update_Tag_When_Value_Updated()
        {
            var persons = Fakers.CreateFakePerson().Generate(1);
            var person = persons.First();

            var entityTable = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString)
                .Configure(options =>
                {
                    _defaultOptions(options);
                    options.HandleTagMutation = true;
                }
                , c =>
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

            created.Should().NotBeNull();
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
                options => _defaultOptions(options),
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

            var entityTable = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString).Configure(options => _defaultOptions(options), c =>
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
        public async Task Should_Set_Primary_Key_On_InsertOrUpdate()
        {
            var person = Fakers.CreateFakePerson().Generate();
            var tableEntity = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString).Configure(options => _defaultOptions(options), c =>
            {
                c.SetPartitionKey(p => p.TenantId);
                c.SetRowKeyProp(p => p.PersonId);
            });

            await tableEntity.AddOrReplaceAsync(person);
            var created = await tableEntity.GetByIdAsync(person.TenantId, person.PersonId);
            created.Should().BeEquivalentTo(person);
        }

        [TestMethod]
        public async Task Should_Set_Dynamic_Prop_On_InsertOrUpdate()
        {
            static string First3Char(string s) => s.ToLower()[..3];

            var person = Fakers.CreateFakePerson().Generate();
            var tableEntity = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString).Configure(options => _defaultOptions(options), c =>
            {
                c.SetPartitionKey(p => p.TenantId);
                c.SetRowKeyProp(p => p.PersonId);
                c.AddComputedProp("_FirstLastName3Chars", p => First3Char(p.LastName));
            });

            await tableEntity.AddOrReplaceAsync(person);
            var created = await tableEntity.GetByIdAsync(person.TenantId, person.PersonId);
            First3Char(created.LastName).Should().Be(First3Char(person.LastName));
        }

        [TestMethod]
        public async Task Should_Set_Computed_Index_On_InsertOrUpdate()
        {
            static string First3Char(string s) => s.ToLower()[..3];
            var person = Fakers.CreateFakePerson().Generate();
            var tableEntity = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString)
            .Configure(options => _defaultOptions(options), c =>
            {
                c.SetPartitionKey(p => p.TenantId);
                c.SetRowKeyProp(p => p.PersonId);
                c.AddComputedProp("_FirstLastName3Chars", p => First3Char(p.LastName));
                c.AddTag("_FirstLastName3Chars");
            });

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

        [TestMethod]
        public async Task Should_Override_Entity_Prop_With_Computed_Prop()
        {
            var person = Fakers.CreateFakePerson().Generate();

            //arrange entity
            var CreatedDate = DateTimeOffset.UtcNow;
            //
            var tableEntity = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString).Configure(options => _defaultOptions(options), c =>
            {
                c.SetPartitionKey(p => p.TenantId);
                c.SetRowKeyProp(p => p.PersonId);
                c.AddComputedProp(nameof(person.Created), p => CreatedDate);
            });

            await tableEntity.AddOrReplaceAsync(person);
            var created = await tableEntity.GetByIdAsync(person.TenantId, person.PersonId);
            created.Created.Should().Be(CreatedDate);
        }

        [TestMethod]
        public async Task Should_Use_Entity_Prop_as_Computed_Prop()
        {
            var person = Fakers.CreateFakePerson().Generate();

            //arrange entity
            var CreatedDate = DateTimeOffset.UtcNow;
            person.Created = null;

            var tableEntity = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString).Configure(options => _defaultOptions(options), c =>
            {
                c.SetPartitionKey(p => p.TenantId);
                c.SetRowKeyProp(p => p.PersonId);
                c.AddComputedProp(nameof(person.Created), p => p.Created ?? CreatedDate);
            });

            await tableEntity.AddOrReplaceAsync(person);
            var created = await tableEntity.GetByIdAsync(person.TenantId, person.PersonId);
            created.Created.Should().Be(CreatedDate);
        }

        [TestMethod]
        public async Task Should_Initialize_Computed_Prop_Only_When_Entity_Created()
        {
            var person = Fakers.CreateFakePerson().Generate();

            //arrange entity

            var tableEntity = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString)
            .Configure(options => _defaultOptions(options), c =>
            {
                c.SetPartitionKey(p => p.TenantId);
                c.SetRowKeyProp(p => p.PersonId);
                c.AddComputedProp(nameof(person.CreatedEntity), p => p.CreatedEntity ?? DateTimeOffset.UtcNow);
                c.AddComputedProp(nameof(person.Updated), p => DateTimeOffset.UtcNow);
            });

            await tableEntity.AddOrReplaceAsync(person);
            var createdEntity = await tableEntity.GetByIdAsync(person.TenantId, person.PersonId);
            createdEntity.Created.Should().NotBeNull();

            var personPatch = new PersonEntity()
            {
                TenantId = person.TenantId,
                PersonId = person.PersonId
            };
            await tableEntity.MergeAsync(personPatch);
            var mergedEntity = await tableEntity.GetByIdAsync(personPatch.TenantId, personPatch.PersonId);

            mergedEntity.CreatedEntity.Should().Be(createdEntity.CreatedEntity);
            mergedEntity.Updated.Should().BeAfter(createdEntity.CreatedEntity ?? default);

            await tableEntity.ReplaceAsync(person);
            var replacedEntity = await tableEntity.GetByIdAsync(person.TenantId, person.PersonId);

            replacedEntity.CreatedEntity.Should().Be(createdEntity.CreatedEntity);
            replacedEntity.Updated.Should().BeAfter(mergedEntity.Updated);
        }

        [TestMethod]
        public async Task Should_Remove_Tags_OnDelete()
        {
            static string First3Char(string s) => s.ToLower()[..3];

            var person = Fakers.CreateFakePerson().Generate();

            var tableEntity = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString).Configure(options => _defaultOptions(options), c =>
            {
                c.SetPartitionKey(p => p.TenantId);
                c.SetRowKeyProp(p => p.PersonId);
                c.AddTag("_FirstLastName3Chars");
                c.AddTag(p => p.LastName);
                c.AddComputedProp("_FirstLastName3Chars", p => First3Char(p.LastName));
            });

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

        [TestMethod]
        public async Task Should_Observe_Entity_Table_Updates()
        {
            var persons = Fakers.CreateFakePerson().Generate(10);
            var observer = new DummyObserver();

            var tableEntity = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString).Configure(options => _defaultOptions(options), c =>
            {
                c.SetPartitionKey(p => p.TenantId)
                .SetRowKeyProp(p => p.PersonId)
                .AddTag(p => p.LastName)
                .AddObserver(nameof(DummyObserver), () => observer);
            });

            await tableEntity.AddManyAsync(persons);

            await tableEntity.DeleteAsync(persons.Skip(1).First());

            observer.Persons.Should().HaveCount(9);
            observer.CreatedCount.Should().Be(10);
            observer.DeletedCount.Should().Be(1);
        }

        [TestMethod]
        public async Task Should_Insert_Many_Indexed_Entities()
        {
            var persons = Fakers.CreateFakePerson().Generate(130);

            //force entities to have same partition (tenantId)
            var partitionName = Guid.NewGuid().ToString();
            persons.ForEach(p => p.TenantId = partitionName);

            IEntityTableClient<PersonEntity> tableEntity = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString).Configure(options => _defaultOptions(options), config =>
            {
                config
                .SetPartitionKey(p => p.TenantId)
                .SetRowKeyProp(p => p.PersonId)
                .AddTag(p => p.LastName)
                .AddTag(p => p.Created);
            });

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

        [TestMethod]
        public async Task Should_Update_Many_Indexed_Entities()
        {
            var persons = Fakers.CreateFakePerson().Generate(130);

            //force entities to have same partition (tenantId)
            var partitionName = Guid.NewGuid().ToString();
            persons.ForEach(p => p.TenantId = partitionName);

            IEntityTableClient<PersonEntity> tableEntity = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString).Configure(options => _defaultOptions(options), config =>
            {
                config
                .SetPartitionKey(p => p.TenantId)
                .SetRowKeyProp(p => p.PersonId)
                .AddTag(p => p.LastName)
                .AddTag(p => p.Created);
            });

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

        [TestMethod]
        public async Task Should_Update_Many_Indexed_Entities_With_No_Existing_Entities()
        {
            var persons = Fakers.CreateFakePerson().Generate(130);

            //force entities to have same partition (tenantId)
            var partitionName = Guid.NewGuid().ToString();
            persons.ForEach(p => p.TenantId = partitionName);

            IEntityTableClient<PersonEntity> tableEntity = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString).Configure(options => _defaultOptions(options), config =>
            {
                config
                .SetPartitionKey(p => p.TenantId)
                .SetRowKeyProp(p => p.PersonId)
                .AddTag(p => p.LastName)
                .AddTag(p => p.Created);
            });

            await tableEntity.CreateTableAsync();
            var updated = await tableEntity.UpdateManyAsync(person =>
            {
                person.LastName += "updated";
            });
            updated.Should().Be(0);
        }

        [TestMethod]
        public async Task Should_Store_Default_DateTime_Values()
        {
            var person = Fakers.CreateFakePerson().Generate(1).FirstOrDefault();

            person.Created = default;
            person.Updated = default;
            person.LocalCreated = default;
            person.LocalUpdated = default;

            var entityTable = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString).Configure(options => _defaultOptions(options), c =>
            {
                c
                .SetPartitionKey(p => p.TenantId)
                .SetRowKeyProp(p => p.PersonId)
                .AddTag(p => p.LastName)
                .AddTag(p => p.Created);
            });

            await entityTable.AddOrReplaceAsync(person);
            var added = await entityTable.GetByIdAsync(person.TenantId, person.PersonId);
            added.Should().BeEquivalentTo(person);
        }

        [TestMethod]
        public async Task Should_Store_Null_DateTime_Values()
        {
            var person = Fakers.CreateFakePerson().Generate(1).FirstOrDefault();

            person.Created = null;
            person.LocalCreated = null;

            var entityTable = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString).Configure(options => _defaultOptions(options), c =>
            {
                c
                .SetPartitionKey(p => p.TenantId)
                .SetRowKeyProp(p => p.PersonId)
                .AddTag(p => p.LastName)
                .AddTag(p => p.Created);
            });

            await entityTable.AddOrReplaceAsync(person);
            var added = await entityTable.GetByIdAsync(person.TenantId, person.PersonId);
            added.Should().BeEquivalentTo(person);
        }

        [TestMethod]
        public async Task Should_Escape_Not_Supported_Char_For_PartitionKeys()
        {
            var person = Fakers.CreateFakePerson().Generate(1).FirstOrDefault();

            person.TenantId = "/\\#?Tenant123!\n\t\r\0\u0007\u009f4";
            person.Created = null;
            person.LocalCreated = null;

            var entityTable = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString)
            .Configure(options => _defaultOptions(options), c =>
            {
                c
                .SetPartitionKey(p => p.TenantId)
                .SetRowKeyProp(p => p.PersonId)
                .AddTag(p => p.LastName)
                .AddTag(p => p.Created);
            });

            await entityTable.AddOrReplaceAsync(person);
            var added = await entityTable.GetByIdAsync(person.TenantId, person.PersonId);
            added.Should().BeEquivalentTo(person);

            var direct = await entityTable.GetAsync(f => f.WherePartitionKey().Equal("****Tenant123!******4")).FirstAsync();
            direct.Should().BeEquivalentTo(person);
        }

        [TestMethod]
        public async Task Should_Escape_Not_Supported_Char_For_RowKeys()
        {
            var person = Fakers.CreateFakePerson().Generate(1).FirstOrDefault();

            person.LastName = "/\\#?Person123!\n\t\r\0\u0007\u009f4";
            person.Created = null;
            person.LocalCreated = null;

            var entityTable = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString)
             .Configure(options => _defaultOptions(options), c =>
            {
                c
                .SetPartitionKey(p => p.TenantId)
                .SetRowKeyProp(p => p.LastName)
                .AddTag(p => p.LastName)
                .AddTag(p => p.Created);
            });

            await entityTable.AddOrReplaceAsync(person);
            var added = await entityTable.GetByIdAsync(person.TenantId, person.LastName);
            added.Should().BeEquivalentTo(person);
            var mainRow = await entityTable.GetAsync(f => f.WhereRowKey().Equal("****Person123!******4")).FirstOrDefaultAsync();
            mainRow.Should().BeEquivalentTo(person);

            var createdRow = await entityTable.GetAsync(f => f.WhereTag("LastName").Equal(person.LastName)).FirstOrDefaultAsync();
            createdRow.Should().BeEquivalentTo(person);

            var lastNameRow = await entityTable.GetAsync(f => f.WhereTag("Created").Equal(person.Created)).FirstOrDefaultAsync();
            lastNameRow.Should().BeEquivalentTo(person);
        }

        [TestMethod]
        public async Task Should_Serialize_Enum_As_String()
        {
            var person = Fakers.CreateFakePerson().Generate(1).FirstOrDefault();
            var options = new EntityTableClientOptions() { };
            _defaultOptions.Invoke(options);
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

        [TestMethod]
        public async Task Should_Deserialize_Existing_Enum_As_Integer()
        {
            var person = Fakers.CreateFakePerson().Generate(1).FirstOrDefault();
            var options = new EntityTableClientOptions() { };
            _defaultOptions.Invoke(options);

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

        [TestMethod]
        public async Task Should_Partially_Delete_Entity_After_Partial_Merge()
        {
            var persons = Fakers.CreateFakePerson().Generate(1);
            var person = persons.First();

            var entityTable = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString)
                .Configure(options => _defaultOptions(options), c =>
                {
                    c.
                     SetPartitionKey(p => p.TenantId)
                    .SetRowKeyProp(p => p.PersonId)
                    .AddTag(p => p.LastName)
                    .AddTag(p => p.Created);
                });
            await entityTable.AddOrMergeAsync(person);

            var personToMerge = new PersonEntity()
            {
                PersonId = person.PersonId,
                TenantId = person.TenantId
            };

            await entityTable.MergeAsync(personToMerge);

            await entityTable.DeleteByIdAsync(person.TenantId, person.PersonId);

            var deleted = await entityTable.GetByIdAsync(person.TenantId, person.PersonId);

            deleted.Should().BeNull();
        }

        [TestMethod]
        public async Task Should_DeleteById_Entity()
        {
            var persons = Fakers.CreateFakePerson().Generate(1);
            var person = persons.First();

            var entityTable = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString)
                .Configure(options => _defaultOptions(options), c =>
                {
                    c.
                     SetPartitionKey(p => p.TenantId)
                    .SetRowKeyProp(p => p.PersonId)
                    .AddTag(p => p.LastName)
                    .AddTag(p => p.Created);
                });
            await entityTable.AddOrMergeAsync(person);

            var created = await entityTable.GetByIdAsync(person.TenantId, person.PersonId);

            person.Enabled = !person.Enabled;

            await entityTable.MergeAsync(person);

            await entityTable.DeleteByIdAsync(person.TenantId, person.PersonId);
            var deleted = await entityTable.GetByIdAsync(person.TenantId, person.PersonId);

            deleted.Should().BeNull();
        }

        [TestMethod]
        public async Task Should_Delete_Entity()
        {
            var persons = Fakers.CreateFakePerson().Generate(1);
            var person = persons.First();

            var entityTable = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString)
                .Configure(options => _defaultOptions(options), c =>
                {
                    c.
                     SetPartitionKey(p => p.TenantId)
                    .SetRowKeyProp(p => p.PersonId)
                    .AddTag(p => p.LastName)
                    .AddTag(p => p.Created);
                });
            await entityTable.AddAsync(person);

            var isDeleted = await entityTable.DeleteAsync(person);

            var deleted = await entityTable.GetByIdAsync(person.TenantId, person.PersonId);

            isDeleted.Should().BeTrue();
            deleted.Should().BeNull();
        }

        [TestMethod]
        public async Task Should_Delete_When_Entity_Not_Found()
        {
            var persons = Fakers.CreateFakePerson().Generate(1);
            var person = persons.First();

            var entityTable = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString)
                .Configure(options => _defaultOptions(options), c =>
                {
                    c.
                     SetPartitionKey(p => p.TenantId)
                    .SetRowKeyProp(p => p.PersonId)
                    .AddTag(p => p.LastName)
                    .AddTag(p => p.Created);
                });
            await entityTable.AddAsync(person);

            var isDeleted = await entityTable.DeleteAsync(person);

            isDeleted.Should().BeTrue();

            var isAlreadyDeleted = await entityTable.DeleteAsync(person);

            isAlreadyDeleted.Should().BeFalse();
        }

        [TestMethod]
        public async Task Should_Delete_By_Id_When_Entity_Not_Found()
        {
            var persons = Fakers.CreateFakePerson().Generate(1);
            var person = persons.First();

            var entityTable = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString)
                .Configure(options => _defaultOptions(options), c =>
                {
                    c.
                     SetPartitionKey(p => p.TenantId)
                    .SetRowKeyProp(p => p.PersonId)
                    .AddTag(p => p.LastName)
                    .AddTag(p => p.Created);
                });
            await entityTable.AddAsync(person);

            var isDeleted = await entityTable.DeleteByIdAsync(person.TenantId, person.PersonId);

            isDeleted.Should().BeTrue();

            var isAlreadyDeleted = await entityTable.DeleteAsync(person);

            isAlreadyDeleted.Should().BeFalse();
        }

        [TestMethod]
        public async Task Should_Delete_By_Id_With_Composite_RowKey()
        {
            var persons = Fakers.CreateFakePerson().Generate(1);
            var person = persons.First();

            var entityTable = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString)
                .Configure(options => _defaultOptions(options), c =>
                {
                    c.
                     SetPartitionKey(p => p.TenantId)
                    .SetRowKey(p => $"{p.PersonId}-{p.Created:aammdd}")
                    .AddTag(p => p.LastName)
                    .AddTag(p => p.Created);
                });

            await entityTable.AddAsync(person);

            var isDeleted = await entityTable.DeleteByIdAsync(person.TenantId, $"{person.PersonId}-{person.Created:aammdd}");

            isDeleted.Should().BeTrue();
        }

        [TestMethod]
        public async Task Should_Delete_Entity_With_Composite_RowKey()
        {
            var persons = Fakers.CreateFakePerson().Generate(1);
            var person = persons.First();

            var entityTable = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString)
                .Configure(options => _defaultOptions(options), c =>
                {
                    c.
                     SetPartitionKey(p => p.TenantId)
                    .SetRowKey(p => $"{p.PersonId}-{p.Created:aammdd}")
                    .AddTag(p => p.LastName)
                    .AddTag(p => p.Created);
                });

            await entityTable.AddAsync(person);

            var isDeleted = await entityTable.DeleteAsync(person);

            isDeleted.Should().BeTrue();
        }

        [TestMethod]
        public void Should_Throw_Exception_When_Adding_Invalid_Keys()
        {
            var person = Fakers.CreateFakePerson().Generate(1).First();
            person.TenantId = null;

            var entityTable = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString).Configure(options => _defaultOptions(options), c =>
            {
                c.
                SetPartitionKey(p => p.TenantId)
                .SetRowKeyProp(p => p.PersonId)
                .AddTag(p => p.LastName)
                .AddTag(p => p.Created);
            });
            Action addAction = () => entityTable.AddAsync(person).GetAwaiter().GetResult();

            addAction.Should()
                .Throw<EntityTableClientException>()
                .WithMessage("Given partitionKey is null");

            person.TenantId = "tenant1";
            person.PersonId = null;

            addAction.Should()
                .Throw<EntityTableClientException>()
                .WithMessage("Given primaryKey is null");
        }
    }
}