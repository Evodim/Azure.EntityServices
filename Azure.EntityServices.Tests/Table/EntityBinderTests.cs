using Azure.Data.Tables;
using Azure.EntityServices.Table.Core;
using Azure.EntityServices.Table.Extensions;
using Azure.EntityServices.Tests.Common;
using Azure.EntityServices.Tests.Common.Fakes;
using Azure.EntityServices.Tests.Common.Helpers;
using Azure.EntityServices.Tests.Common.Models;
using FluentAssertions;
using System;
using System.Threading.Tasks;

namespace Azure.EntityServices.Tests.Table
{
    public class EntityBinderTests
    {
        public EntityBinderTests()
        {
        }

        [PrettyFact]
        public async Task Should_Handle_Extented_Values_Wit_hBindable_Entity()
        {
            var partitionName = Guid.NewGuid().ToShortGuid();

            var person = Fakers.CreateFakePerson().Generate();
            //decimal
            person.Altitude = 1.6666666666666666666666666667M;
            //float
            person.BankAmount = 2.00000024F;
            //enum
            person.Situation = Situation.Divorced;

            var tableEntity = new EntityTableBinder<PersonEntity>(person, partitionName, person.PersonId.ToString());
            using var client = await CreateTemporaryTableClientAsync();
            var result = await ReplaceThenRetrieveAsync(client.Value, tableEntity.Bind());

            var binderResult = new EntityTableBinder<PersonEntity>(result);

            var entity = binderResult.UnBind();

            entity.Altitude.Should().Be(person.Altitude);
            entity.BankAmount.Should().Be(person.BankAmount);
            entity.Situation.Should().Be(person.Situation);
        }

        [PrettyFact]
        public async Task Should_InsertOrMerge_Bindable_Entity()
        {
            var partitionName = Guid.NewGuid().ToString();

            var person = Fakers.CreateFakePerson().Generate();
            var binder = new EntityTableBinder<PersonEntity>(person, partitionName, person.PersonId.ToString());

            using var client = await CreateTemporaryTableClientAsync();
            await ReplaceThenRetrieveAsync(client.Value, binder.Bind());
            binder = new EntityTableBinder<PersonEntity>(new PersonEntity() { PersonId = person.PersonId, FirstName = "John Do" }, partitionName, person.PersonId.ToString());

            var merged = await MergeThenRetrieveAsync(client.Value, binder.Bind());
            var binderResult = new EntityTableBinder<PersonEntity>(merged);
            var entity = binderResult.UnBind();

            //Only Nullable value and reference types are preserved in merge operation
            entity.LastName.Should().Be(person.LastName);
            entity.Latitude.Should().Be(default);
            entity.Longitude.Should().Be(default);
            entity.Altitude.Should().Be(person.Altitude);
            entity.BankAmount.Should().Be(person.BankAmount);
            entity.PersonId.Should().Be(person.PersonId.ToString());
            entity.FirstName.Should().Be("John Do");
        }

        [PrettyFact]
        public async Task Should_InsertOrReplace_Bindable_Entity()
        {
            using var client = await CreateTemporaryTableClientAsync();
            var partitionName = Guid.NewGuid().ToString();

            var person = Fakers.CreateFakePerson().Generate();
            var binder = new EntityTableBinder<PersonEntity>(person, partitionName, person.PersonId.ToString());

            var replaced = await ReplaceThenRetrieveAsync(client.Value, binder.Bind());
            var binderResult = new EntityTableBinder<PersonEntity>(replaced);

            binderResult.UnBind();

            binderResult.RowKey.Should().Be(person.PersonId.ToString());
            binderResult.Entity.Should().BeEquivalentTo(person);
        }

        [PrettyFact]
        public async Task Should_InsertOrReplace_Metadatas_With_Bindable_Entity()
        {
            using var client = await CreateTemporaryTableClientAsync();

            var partitionName = Guid.NewGuid().ToString();
            var person = Fakers.CreateFakePerson().Generate();

            var binder = new EntityTableBinder<PersonEntity>(person, partitionName, person.PersonId.ToString());
            binder.Metadata.Add("_HasChildren", true);
            binder.Metadata.Add("_Deleted", false);

            await ReplaceThenRetrieveAsync(client.Value, binder.Bind());
            binder = new EntityTableBinder<PersonEntity>(person, partitionName, person.PersonId.ToString());
            binder.Metadata.Add("_HasChildren", false);

            var replaced = await ReplaceThenRetrieveAsync(client.Value, binder.Bind());
            var binderResult = new EntityTableBinder<PersonEntity>(replaced);
            binderResult.UnBind();

            binderResult.Entity.Should().BeEquivalentTo(person);
            binderResult.Metadata.Should().Contain("_HasChildren", false);
            binderResult.Metadata.Should().NotContainKey("_Deleted", because: "InsertOrReplace replace all entity props and it's metadatas");
        }

        [PrettyFact]
        public async Task Should_Merge_Metadatas_With_Bindable_Entity()
        {
            var client = await CreateTemporaryTableClientAsync();
            var partitionName = Guid.NewGuid().ToString();

            var person = Fakers.CreateFakePerson().Generate();

            var binder = new EntityTableBinder<PersonEntity>(person, partitionName, person.PersonId.ToString());
            binder.Metadata.Add("_HasChildren", true);
            binder.Metadata.Add("_Deleted", true);
            binder.Bind();

            await ReplaceThenRetrieveAsync(client.Value, binder.Bind());

            binder = new EntityTableBinder<PersonEntity>(person, partitionName, person.PersonId.ToString());
            binder.Metadata.Add("_HasChildren", false);

            var merged = await MergeThenRetrieveAsync(client.Value, binder.Bind());
            var binderResult = new EntityTableBinder<PersonEntity>(merged);

            binderResult.UnBind();

            binderResult.Entity.Should().BeEquivalentTo(person);
            binderResult.Metadata.Should().Contain("_HasChildren", false);
            binderResult.Metadata.Should().ContainKey("_Deleted", because: "InsertOrMerge preserve non updated prop and metadatas");
            binderResult.Metadata.Should().Contain("_Deleted", true);
        }

        [PrettyFact]
        public async Task Should_Store_Nullable_Types_In_Bindable_Entity()
        {
            var partitionName = Guid.NewGuid().ToString();
            var person = Fakers.CreateFakePerson().Generate();
            var client = new TableClient(TestEnvironment.ConnectionString, $"{nameof(EntityBinderTests)}{Guid.NewGuid():N}");
            client.CreateIfNotExistsAsync().Wait();
            person.Altitude = null;
            person.Distance = default;
            person.Created = null;
            person.Situation = null;

            var binder = new EntityTableBinder<PersonEntity>(person, partitionName, person.PersonId.ToString());

            await client.UpsertEntityAsync(binder.Bind());
            var created = await client.GetEntityAsync<TableEntity>(binder.PartitionKey, binder.RowKey);

            var createdEntity = new EntityTableBinder<PersonEntity>(created).UnBind();

            createdEntity.Altitude.Should().Be(person.Altitude);
            createdEntity?.Distance.Should().Be(person.Distance);
        }

        private static async Task<TableEntity> MergeThenRetrieveAsync<T>(TableClient client, T tableEntity)
            where T : class, ITableEntity, new()

        {
            await client.UpsertEntityAsync(tableEntity, TableUpdateMode.Merge);
            return await client.GetEntityAsync<TableEntity>(tableEntity.PartitionKey, tableEntity.RowKey);
        }

        private static async Task<TableEntity> ReplaceThenRetrieveAsync<T>(TableClient client, T tableEntity)
           where T : class, ITableEntity, new()
        {
            await client.UpsertEntityAsync(tableEntity, TableUpdateMode.Replace);
            return await client.GetEntityAsync<TableEntity>(tableEntity.PartitionKey, tableEntity.RowKey);
        }

        private static async Task<DisposableContext<TableClient>> CreateTemporaryTableClientAsync()
        {
            var client = new TableClient(TestEnvironment.ConnectionString, $"{nameof(EntityBinderTests)}{Guid.NewGuid():N}".ToLowerInvariant());
            await client.CreateIfNotExistsAsync();

            return new DisposableContext<TableClient>(client, e => client.DeleteAsync());
        }
    }
}