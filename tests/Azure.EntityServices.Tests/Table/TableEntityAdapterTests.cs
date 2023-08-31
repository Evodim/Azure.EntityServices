using Azure.Data.Tables;
using Azure.EntityServices.Tables;
using Azure.EntityServices.Tables.Core;
using Common.Samples;
using Common.Samples.Models;
using Common.Samples.Tools.Fakes;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading.Tasks;

namespace Azure.EntityServices.Table.Tests
{
    [TestClass]
    public class TableEntityAdapterTests
    {
        public TableEntityAdapterTests()
        {
        }

        //define an entity model with tenanid as partition key and personId as primary key
        private readonly EntityKeyBuilder<PersonEntity> _entityKeyBuilder
           = new(e => e.TenantId, e => e.PersonId);

        [TestMethod]
        public async Task Should_Adapt_Entity_Extented_Storage_Types()
        {
            var person = Fakers.CreateFakePerson().Generate();
            //decimal
            person.Altitude = 1.6666666666666666666666666667M;
            //float
            person.BankAmount = 2.00000024F;
            //enum
            person.Situation = Situation.Divorced;

            var adapter = new TableEntityAdapter<PersonEntity>(_entityKeyBuilder);
            var client = new Data.Tables.TableClient(TestEnvironment.ConnectionString, NewTableName());

            await client.CreateIfNotExistsAsync();

            var result = await UpsertAndGetEntity(client, adapter.ToEntityModel(person));

            var entity = adapter.FromEntityModel(result);

            entity.Should().BeEquivalentTo(person);
        }

        [TestMethod]
        public async Task Should_Adapt_Entity_InsertOrMerge()
        {
            var person = Fakers.CreateFakePerson().Generate();
            person.Enabled = true;

            var adapter = new TableEntityAdapter<PersonEntity>(_entityKeyBuilder);

            var client = new Data.Tables.TableClient(TestEnvironment.ConnectionString, NewTableName());

            await client.CreateIfNotExistsAsync();

            await UpsertAndGetEntity(client, adapter.ToEntityModel(person));
            var personToMerge = new PersonEntity()
            {
                TenantId = person.TenantId,
                PersonId = person.PersonId,
                FirstName = "John Do",
                LocalCreated = null,
                LocalUpdated = default,
                Updated = default,
                Enabled = false
            };

            var merged = await MergeThenRetrieveAsync(client, adapter.ToEntityModel(person));
            var entity = adapter.FromEntityModel(merged);

            //Only Nullable value and reference types are preserved in merge operation
            entity.Should().BeEquivalentTo(person);
        }

        [TestMethod]
        public async Task Should_Adapt_Entity_On_Update()
        {
            var client = new Data.Tables.TableClient(TestEnvironment.ConnectionString, NewTableName());

            await client.CreateIfNotExistsAsync();

            var person = Fakers.CreateFakePerson().Generate();
            var adapter = new TableEntityAdapter<PersonEntity>(_entityKeyBuilder);

            var replaced = await UpsertAndGetEntity(client, adapter.ToEntityModel(person));

            var entity = adapter.FromEntityModel(replaced);

            entity.Should().BeEquivalentTo(person);
        }

        [TestMethod]
        public async Task Should_Adapt_Entity_Metadatas_On_Update()
        {
            var client = new Data.Tables.TableClient(TestEnvironment.ConnectionString, NewTableName());

            await client.CreateIfNotExistsAsync();

            var person = Fakers.CreateFakePerson().Generate();

            var metadata = new Dictionary<string, Func<PersonEntity, object>>()
            {
                ["_HasChildren"] = e => true,
                ["_Deleted"] = e => false
            };
            var metadataWithMissingProp = new Dictionary<string, Func<PersonEntity, object>>()
            {
                ["_HasChildren"] = e => false
            };

            var adapter = new TableEntityAdapter<PersonEntity>(_entityKeyBuilder, computedProps: metadata);
            var adapterWithMissingProp = new TableEntityAdapter<PersonEntity>(_entityKeyBuilder, computedProps: metadataWithMissingProp);

            var tableEntity = await UpsertAndGetEntity(client, adapter.ToEntityModel(person));
            var entity = adapter.FromEntityModel(tableEntity);

            var replaced = await UpsertAndGetEntity(client, adapterWithMissingProp.ToEntityModel(person));

            var replaceEntity = adapter.FromEntityModel(replaced);

            replaceEntity.Should().BeEquivalentTo(person);

            var props = adapter.GetProperties(replaced);

            props.Keys.Should().Contain("_HasChildren");
            props.Keys.Should().NotContain("_Deleted");

            props["_HasChildren"].Should().Be(false);
        }

        [TestMethod]
        public async Task Should_Adapt_Entity_Metadatas_On_Merge()
        {
            var client = new Data.Tables.TableClient(TestEnvironment.ConnectionString, NewTableName());

            await client.CreateIfNotExistsAsync();

            var person = Fakers.CreateFakePerson().Generate();

            var metadata = new Dictionary<string, Func<PersonEntity, object>>()
            {
                ["_HasChildren"] = e => true,
                ["_Deleted"] = e => false
            };
            var metadataWithMissingProp = new Dictionary<string, Func<PersonEntity, object>>()
            {
                ["_HasChildren"] = e => false,
                ["_AnotherProp"] = e => 0.14545
            };

            var adapter = new TableEntityAdapter<PersonEntity>(_entityKeyBuilder, computedProps: metadata);
            var adapterWithMissingProp = new TableEntityAdapter<PersonEntity>(_entityKeyBuilder, computedProps: metadataWithMissingProp);

            await UpsertAndGetEntity(client, adapter.ToEntityModel(person));

            var merged = await MergeThenRetrieveAsync(client, adapterWithMissingProp.ToEntityModel(person));

            var mergedEntity = adapter.FromEntityModel(merged);

            mergedEntity.Should().BeEquivalentTo(person);

            var props = adapter.GetProperties(merged);

            props.Keys.Should().Contain("_HasChildren");
            props.Keys.Should().Contain("_Deleted");
            props.Keys.Should().Contain("_AnotherProp");

            props["_HasChildren"].Should().Be(false);
            props["_Deleted"].Should().Be(false);
            props["_AnotherProp"].Should().Be(0.14545);
        }

        [TestMethod]
        public async Task Should_Adapt_Entity_ComputedProps()
        {
            var client = new Data.Tables.TableClient(TestEnvironment.ConnectionString, NewTableName());

            var computedProps = new Dictionary<string, Func<PersonEntity, object>>() { ["_distance_less_than_500m"] = (e) => e.Distance < 500 };

            var adapter = new TableEntityAdapter<PersonEntity>(_entityKeyBuilder, computedProps: computedProps);

            await client.CreateIfNotExistsAsync();

            var person = Fakers.CreateFakePerson().Generate();

            person.Distance = 250;

            var added = await UpsertAndGetEntity(client, adapter.ToEntityModel(person));

            var props = adapter.GetProperties(added);

            props.Should().ContainKey("_distance_less_than_500m");
            (props["_distance_less_than_500m"] as bool?)?.Should().BeTrue();

            person.Distance = 501;

            var replaced = await UpsertAndGetEntity(client, adapter.ToEntityModel(person));

            var replacedProps = adapter.GetProperties(replaced);

            replacedProps.Should().ContainKey("_distance_less_than_500m");
            (replacedProps["_distance_less_than_500m"] as bool?)?.Should().BeFalse();
        }

        [TestMethod]
        public async Task Should_Adapt_Entity_With_String_Interpolation()
        {
            var person = Fakers.CreateFakePerson();

            var client = new Data.Tables.TableClient(TestEnvironment.ConnectionString, NewTableName());

            await client.CreateIfNotExistsAsync();
            var localDate = DateTime.Now;
            var utcDate = DateTime.UtcNow;
            var localOffsetDate = DateTimeOffset.Now;
            var utcOffsetDate = DateTimeOffset.UtcNow;

            var metadata = new Dictionary<string, Func<PersonEntity, object>>()
            {
                ["LocalCreated"] = e => localDate.ToString("O", CultureInfo.InvariantCulture),
                ["LocalUpdated"] = e => utcDate.ToString("O", CultureInfo.InvariantCulture),
                ["Created"] = e => localOffsetDate.ToString("O", CultureInfo.InvariantCulture),
                ["Updated"] = e => utcOffsetDate.ToString("O", CultureInfo.InvariantCulture),
            };
            var adapter = new TableEntityAdapter<PersonEntity>(_entityKeyBuilder, computedProps: metadata);

            var added = adapter.ToEntityModel(person);
            var result = adapter.FromEntityModel(added);

            result.LocalCreated.Should().Be(localDate.ToUniversalTime(), because: "Only UTC date could be stored properly without local offset mismatch");
            result.LocalUpdated.Should().Be(utcDate);
            result.Created.Should().Be(localOffsetDate);
            result.Updated.Should().Be(utcOffsetDate);
        }

        [TestMethod]
        public async Task Should_Adapt_Entity_Nullable_Types()
        {
            var person = Fakers.CreateFakePerson().Generate();

            var client = new Data.Tables.TableClient(TestEnvironment.ConnectionString, NewTableName());

            await client.CreateIfNotExistsAsync();

            person.Altitude = null;
            person.Distance = default;
            person.Created = null;
            person.Situation = null;

            var adapter = new TableEntityAdapter<PersonEntity>(_entityKeyBuilder);

            var added = await UpsertAndGetEntity(client, adapter.ToEntityModel(person));

            var addedEntity = adapter.FromEntityModel(added);

            addedEntity.Should().BeEquivalentTo(person);
        }

        [TestMethod]
        public async Task Should_Adapt_Entity_With_Ignored_Props()
        {
            var person = Fakers.CreateFakePerson().Generate();

            var propsToIgnore = new List<string>()
            {
               nameof(PersonEntity.FirstName),
               nameof(PersonEntity.LastName),
               nameof(PersonEntity.Latitude),
               nameof(PersonEntity.Longitude)
            };

            var adapter = new TableEntityAdapter<PersonEntity>(_entityKeyBuilder, propsToIgnore: propsToIgnore);
            var client = new Data.Tables.TableClient(TestEnvironment.ConnectionString, NewTableName());

            await client.CreateIfNotExistsAsync();

            var result = await UpsertAndGetEntity(client, adapter.ToEntityModel(person));

            var entity = adapter.FromEntityModel(result);

            entity.Should().BeEquivalentTo(person, options => options
            .Excluding(p => p.FirstName)
            .Excluding(p => p.LastName)
            .Excluding(p => p.Latitude)
            .Excluding(p => p.Longitude)
            );

            entity.FirstName.Should().BeNull();
            entity.LastName.Should().BeNull();
            entity.Latitude.Should().Be(default);
            entity.Longitude.Should().Be(default);
        }

        private static async Task<TableEntity> MergeThenRetrieveAsync<T>(Data.Tables.TableClient client, T tableEntity)
            where T : class, ITableEntity, new()

        {
            await client.UpsertEntityAsync(tableEntity, TableUpdateMode.Merge);
            return await client.GetEntityAsync<TableEntity>(tableEntity.PartitionKey, tableEntity.RowKey);
        }

        private static async Task<TableEntity> UpsertAndGetEntity<T>(Data.Tables.TableClient client, T tableEntity)
           where T : class, ITableEntity, new()
        {
            await client.UpsertEntityAsync(tableEntity, TableUpdateMode.Replace);
            return await client.GetEntityAsync<TableEntity>(tableEntity.PartitionKey, tableEntity.RowKey);
        }

        private static string NewTableName() => $"{nameof(TableEntityAdapterTests)}{Guid.NewGuid():N}".ToLowerInvariant();
    }
}