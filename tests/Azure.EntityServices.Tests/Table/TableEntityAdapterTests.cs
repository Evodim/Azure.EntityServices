using Azure.Data.Tables;
using Azure.EntityServices.Tables;
using Azure.EntityServices.Tables.Core;
using Azure.EntityServices.Tables.Extensions;
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

            var tableEntity = new TableEntityAdapter<PersonEntity>(person, _entityKeyBuilder);
            var client = new Data.Tables.TableClient(TestEnvironment.ConnectionString, NewTableName());

            await client.CreateIfNotExistsAsync();

            var result = await UpsertAndGetEntity(client, tableEntity.WriteToEntityModel());

            var adapterResult = new TableEntityAdapter<PersonEntity>(result, _entityKeyBuilder);

            var entity = adapterResult.ReadFromEntityModel();

            entity.Altitude.Should().Be(person.Altitude);
            entity.BankAmount.Should().Be(person.BankAmount);
            entity.Situation.Should().Be(person.Situation);
        }

        [TestMethod]
        public async Task Should_Adapt_Entity_InsertOrMerge()
        {
            var person = Fakers.CreateFakePerson().Generate();
            person.Enabled = true;

            var adapter = new TableEntityAdapter<PersonEntity>(person, _entityKeyBuilder);

            var client = new Data.Tables.TableClient(TestEnvironment.ConnectionString, NewTableName());

            await client.CreateIfNotExistsAsync();

            await UpsertAndGetEntity(client, adapter.WriteToEntityModel());
            adapter = new TableEntityAdapter<PersonEntity>(new PersonEntity()
            {
                TenantId = person.TenantId,
                PersonId = person.PersonId,
                FirstName = "John Do",
                LocalCreated = null,
                LocalUpdated = default,
                Updated = default,
                Enabled = false
            }, _entityKeyBuilder);

            var merged = await MergeThenRetrieveAsync(client, adapter.WriteToEntityModel());
            var adapterResult = new TableEntityAdapter<PersonEntity>(merged, _entityKeyBuilder);
            var entity = adapterResult.ReadFromEntityModel();

            //Only Nullable value and reference types are preserved in merge operation
            entity.LastName.Should().Be(person.LastName);
            entity.Latitude.Should().Be(default);
            entity.Longitude.Should().Be(default);
            entity.Altitude.Should().Be(person.Altitude);
            entity.Updated.Should().Be(default);
            entity.Created.Should().Be(person.Created);
            entity.LocalCreated.Should().Be(person.LocalCreated);
            entity.LocalUpdated.Should().Be(default);
            entity.Enabled.Should().Be(false);
            entity.BankAmount.Should().Be(person.BankAmount);
            entity.PersonId.Should().Be(person.PersonId.ToString());
            entity.FirstName.Should().Be("John Do");
        }

        [TestMethod]
        public async Task Should_Adapt_Entity_On_Update()
        {
            var client = new Data.Tables.TableClient(TestEnvironment.ConnectionString, NewTableName());

            await client.CreateIfNotExistsAsync();

            var person = Fakers.CreateFakePerson().Generate();
            var adapter = new TableEntityAdapter<PersonEntity>(person, _entityKeyBuilder);

            var replaced = await UpsertAndGetEntity(client, adapter.WriteToEntityModel());
            var adapterResult = new TableEntityAdapter<PersonEntity>(replaced, _entityKeyBuilder);

            adapterResult.ReadFromEntityModel();

            adapterResult.RowKey.Should().Be(person.PersonId.ToString());
            adapterResult.Entity.Should().BeEquivalentTo(person);
        }

        [TestMethod]
        public async Task Should_Adapt_Entity_Metadatas_On_Update()
        {
            var client = new Data.Tables.TableClient(TestEnvironment.ConnectionString, NewTableName());

            await client.CreateIfNotExistsAsync();

            var person = Fakers.CreateFakePerson().Generate();

            var adapter = new TableEntityAdapter<PersonEntity>(person, _entityKeyBuilder);
            adapter.Metadata.Add("_HasChildren", true);
            adapter.Metadata.Add("_Deleted", false);

            await UpsertAndGetEntity(client, adapter.WriteToEntityModel());
            adapter = new TableEntityAdapter<PersonEntity>(person, _entityKeyBuilder);
            adapter.Metadata.Add("_HasChildren", false);

            var replaced = await UpsertAndGetEntity(client, adapter.WriteToEntityModel());
            var adapterResult = new TableEntityAdapter<PersonEntity>(replaced, _entityKeyBuilder);
            adapterResult.ReadFromEntityModel();

            adapterResult.Entity.Should().BeEquivalentTo(person);
            adapterResult.Metadata.Should().Contain("_HasChildren", false);
            adapterResult.Metadata.Should().NotContainKey("_Deleted", because: "InsertOrReplace replace all entity props and it's metadatas");
        }

        [TestMethod]
        public async Task Should_Adapt_Entity_Metadatas_On_Merge()
        {
            var client = new Data.Tables.TableClient(TestEnvironment.ConnectionString, NewTableName());

            await client.CreateIfNotExistsAsync();

            var person = Fakers.CreateFakePerson().Generate();

            var adapter = new TableEntityAdapter<PersonEntity>(person, _entityKeyBuilder);
            adapter.Metadata.Add("_HasChildren", true);
            adapter.Metadata.Add("_Deleted", true);
            adapter.WriteToEntityModel();

            await UpsertAndGetEntity(client, adapter.WriteToEntityModel());

            adapter = new TableEntityAdapter<PersonEntity>(person, _entityKeyBuilder);
            adapter.Metadata.Add("_HasChildren", false);

            var merged = await MergeThenRetrieveAsync(client, adapter.WriteToEntityModel());
            var adapterResult = new TableEntityAdapter<PersonEntity>(merged, _entityKeyBuilder);

            adapterResult.ReadFromEntityModel();

            adapterResult.Entity.Should().BeEquivalentTo(person);
            adapterResult.Metadata.Should().Contain("_HasChildren", false);
            adapterResult.Metadata.Should().ContainKey("_Deleted");
            adapterResult.Metadata.Should().Contain("_Deleted", true);
        }

        [TestMethod]
        public async Task Should_Adapt_Entity_DynamicProps()
        {
            var client = new Data.Tables.TableClient(TestEnvironment.ConnectionString, NewTableName());

            var dynamicProps = new Dictionary<string, Func<PersonEntity, object>>() { ["_distance_less_than_500m"] = (e) => e.Distance < 500 };
            await client.CreateIfNotExistsAsync();

            var person = Fakers.CreateFakePerson().Generate();

            person.Distance = 250;
            var adapter = new TableEntityAdapter<PersonEntity>(person, _entityKeyBuilder);
            adapter.BindDynamicProps(dynamicProps);

            var added = await UpsertAndGetEntity(client, adapter.WriteToEntityModel());
            added.Should().ContainKey("_distance_less_than_500m");
            (added["_distance_less_than_500m"] as bool?)?.Should().BeTrue();

            person.Distance = 501;
            var adapterToUpdate = new TableEntityAdapter<PersonEntity>(person, _entityKeyBuilder);
            adapterToUpdate.BindDynamicProps(dynamicProps);

            var replaced = await UpsertAndGetEntity(client, adapterToUpdate.WriteToEntityModel());

            replaced.Should().ContainKey("_distance_less_than_500m");
            (replaced["_distance_less_than_500m"] as bool?)?.Should().BeFalse();
        }

        [TestMethod]
        public async Task Should_Adapt_Entity_With_String_Interpolation()
        {
            var person = new PersonEntity();

            var client = new Data.Tables.TableClient(TestEnvironment.ConnectionString, NewTableName());

            await client.CreateIfNotExistsAsync();
            var localDate = DateTime.Now;
            var utcDate = DateTime.UtcNow;
            var localOffsetDate = DateTimeOffset.Now;
            var utcOffsetDate = DateTimeOffset.UtcNow;

            var adapter = new TableEntityAdapter<PersonEntity>(person, _entityKeyBuilder);
            adapter.Properties.Add("LocalCreated", localDate.ToString("O", CultureInfo.InvariantCulture));
            adapter.Properties.Add("LocalUpdated", utcDate.ToString("O", CultureInfo.InvariantCulture));
            adapter.Properties.Add("Created", localOffsetDate.ToString("O", CultureInfo.InvariantCulture));
            adapter.Properties.Add("Updated", utcOffsetDate.ToString("O", CultureInfo.InvariantCulture));
            adapter.WriteToEntityModel();
            var result = adapter.ReadFromEntityModel();

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

            var adapter = new TableEntityAdapter<PersonEntity>(person, _entityKeyBuilder);

            await client.UpsertEntityAsync(adapter.WriteToEntityModel());
            var created = await client.GetEntityAsync<TableEntity>(adapter.PartitionKey, adapter.RowKey);

            var createdEntity = new TableEntityAdapter<PersonEntity>(created, _entityKeyBuilder).ReadFromEntityModel();

            createdEntity.Altitude.Should().Be(person.Altitude);
            createdEntity?.Distance.Should().Be(person.Distance);
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