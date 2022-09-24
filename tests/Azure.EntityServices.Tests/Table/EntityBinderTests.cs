using Azure.Data.Tables;
using Azure.EntityServices.Tables.Core;
using Azure.EntityServices.Tables.Extensions;
using Azure.EntityServices.Table.Common;
using Azure.EntityServices.Table.Common.Fakes;
using Azure.EntityServices.Table.Common.Models;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Threading.Tasks;
using Azure.EntityServices.Tests.Common;
using System.Collections.Generic;
using System.Globalization;

namespace Azure.EntityServices.Table.Tests
{
    [TestClass]
    public class EntityBinderTests
    {
        public EntityBinderTests()
        {
        }

        [TestMethod]
        public async Task Should_Bind_Extented_Storage_Types()
        {
            var partitionName = Guid.NewGuid().ToShortGuid();

            var person = Fakers.CreateFakePerson().Generate();
            //decimal
            person.Altitude = 1.6666666666666666666666666667M;
            //float
            person.BankAmount = 2.00000024F;
            //enum
            person.Situation = Situation.Divorced;

            var tableEntity = new TableEntityBinder<PersonEntity>(person, partitionName, person.PersonId.ToString());
            var client = new TableClient(TestEnvironment.ConnectionString, NewTableName());
            try
            {
                await client.CreateIfNotExistsAsync();

                var result = await UpsertAndGetEntity(client, tableEntity.Bind());

                var binderResult = new TableEntityBinder<PersonEntity>(result);

                var entity = binderResult.UnBind();

                entity.Altitude.Should().Be(person.Altitude);
                entity.BankAmount.Should().Be(person.BankAmount);
                entity.Situation.Should().Be(person.Situation);
            }
            finally
            {
                await client.DeleteAsync();
            }
        }

        [TestMethod]
        public async Task Should_Bind_InsertOrMerge()
        {
            var partitionName = Guid.NewGuid().ToString();

            var person = Fakers.CreateFakePerson().Generate();
            person.Enabled = true;

            var binder = new TableEntityBinder<PersonEntity>(person, partitionName, person.PersonId.ToString());

            var client = new TableClient(TestEnvironment.ConnectionString, NewTableName());
            try
            {
                await client.CreateIfNotExistsAsync();

                await UpsertAndGetEntity(client, binder.Bind());
                binder = new TableEntityBinder<PersonEntity>(new PersonEntity()
                {
                    PersonId = person.PersonId,
                    FirstName = "John Do",
                    LocalCreated = null,
                    LocalUpdated = default,
                    Updated = default,
                    Enabled=false
                }, partitionName,

                person.PersonId.ToString()); 

                var merged = await MergeThenRetrieveAsync(client, binder.Bind());
                var binderResult = new TableEntityBinder<PersonEntity>(merged);
                var entity = binderResult.UnBind();

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
            finally
            {
                await client.DeleteAsync();
            }
        }

        [TestMethod]
        public async Task Should_Bind_On_Update()
        {
            var client = new TableClient(TestEnvironment.ConnectionString, NewTableName());
            try
            {
                await client.CreateIfNotExistsAsync();

                var partitionName = Guid.NewGuid().ToString();

                var person = Fakers.CreateFakePerson().Generate();
                var binder = new TableEntityBinder<PersonEntity>(person, partitionName, person.PersonId.ToString());

                var replaced = await UpsertAndGetEntity(client, binder.Bind());
                var binderResult = new TableEntityBinder<PersonEntity>(replaced);

                binderResult.UnBind();

                binderResult.RowKey.Should().Be(person.PersonId.ToString());
                binderResult.Entity.Should().BeEquivalentTo(person);
            }
            finally
            {
                await client.DeleteAsync();
            }
        }
         
        [TestMethod]
        public async Task Should_Bind_Metadatas_On_Update()
        {
            var client = new TableClient(TestEnvironment.ConnectionString, NewTableName());
            try
            {
                await client.CreateIfNotExistsAsync();

                var partitionName = Guid.NewGuid().ToString();
                var person = Fakers.CreateFakePerson().Generate();

                var binder = new TableEntityBinder<PersonEntity>(person, partitionName, person.PersonId.ToString());
                binder.Metadata.Add("_HasChildren", true);
                binder.Metadata.Add("_Deleted", false);

                await UpsertAndGetEntity(client, binder.Bind());
                binder = new TableEntityBinder<PersonEntity>(person, partitionName, person.PersonId.ToString());
                binder.Metadata.Add("_HasChildren", false);

                var replaced = await UpsertAndGetEntity(client, binder.Bind());
                var binderResult = new TableEntityBinder<PersonEntity>(replaced);
                binderResult.UnBind();

                binderResult.Entity.Should().BeEquivalentTo(person);
                binderResult.Metadata.Should().Contain("_HasChildren", false);
                binderResult.Metadata.Should().NotContainKey("_Deleted", because: "InsertOrReplace replace all entity props and it's metadatas");
            }
            finally
            {
                await client.DeleteAsync();
            }
        }
        [TestMethod]
        public async Task Should_Bind_Metadatas_On_Merge()
        {
            var client = new TableClient(TestEnvironment.ConnectionString, NewTableName());
            try
            {
                await client.CreateIfNotExistsAsync();

                var partitionName = Guid.NewGuid().ToString();

                var person = Fakers.CreateFakePerson().Generate();

                var binder = new TableEntityBinder<PersonEntity>(person, partitionName, person.PersonId.ToString());
                binder.Metadata.Add("_HasChildren", true);
                binder.Metadata.Add("_Deleted", true);
                binder.Bind();

                await UpsertAndGetEntity(client, binder.Bind());

                binder = new TableEntityBinder<PersonEntity>(person, partitionName, person.PersonId.ToString());
                binder.Metadata.Add("_HasChildren", false);

                var merged = await MergeThenRetrieveAsync(client, binder.Bind());
                var binderResult = new TableEntityBinder<PersonEntity>(merged);

                binderResult.UnBind();

                binderResult.Entity.Should().BeEquivalentTo(person);
                binderResult.Metadata.Should().Contain("_HasChildren", false);
                binderResult.Metadata.Should().ContainKey("_Deleted");
                binderResult.Metadata.Should().Contain("_Deleted", true);
            }
            finally
            {
                await client.DeleteAsync();
            }
        }
        [TestMethod]
        public async Task Should_Bind_DynamicProps()
        {
            var client = new TableClient(TestEnvironment.ConnectionString, NewTableName());
            try
            {
              
                var dynamicProps = new Dictionary<string, Func<PersonEntity, object>>() { ["_distance_less_than_500m"] = (e) => e.Distance < 500 };
                await client.CreateIfNotExistsAsync();

                var partitionName = Guid.NewGuid().ToString();
                var person = Fakers.CreateFakePerson().Generate();
                
                person.Distance = 250;
                var binder = new TableEntityBinder<PersonEntity>(person, partitionName, person.PersonId.ToString());
                binder.BindDynamicProps(dynamicProps);

                var added=await UpsertAndGetEntity(client, binder.Bind()); 
                added.Should().ContainKey("_distance_less_than_500m");
                (added["_distance_less_than_500m"] as bool?)?.Should().BeTrue();

                person.Distance = 501;
                 var binderToUpdate = new TableEntityBinder<PersonEntity>(person, partitionName, person.PersonId.ToString());
                binderToUpdate.BindDynamicProps(dynamicProps);
                
                var replaced = await UpsertAndGetEntity(client, binderToUpdate.Bind());

                replaced.Should().ContainKey("_distance_less_than_500m");
                (replaced["_distance_less_than_500m"] as bool?)?.Should().BeFalse();  
            }
            finally
            {
                await client.DeleteAsync();
            }
        }

        [TestMethod]
        public async Task Should_Interpolate_String_Entity_Types()
        {
            var partitionName = Guid.NewGuid().ToString();
            var person = new PersonEntity();

            var client = new TableClient(TestEnvironment.ConnectionString, NewTableName());
            try
            {
                await client.CreateIfNotExistsAsync();
                var localDate = DateTime.Now;
                var utcDate = DateTime.UtcNow;
                var localOffsetDate = DateTimeOffset.Now;
                var utcOffsetDate = DateTimeOffset.UtcNow;

                var binder = new TableEntityBinder<PersonEntity>(person, partitionName, person.PersonId.ToString());                
                binder.Properties.Add("LocalCreated", localDate.ToString("O",CultureInfo.InvariantCulture));
                binder.Properties.Add("LocalUpdated", utcDate.ToString("O", CultureInfo.InvariantCulture));
                binder.Properties.Add("Created", localOffsetDate.ToString("O", CultureInfo.InvariantCulture));
                binder.Properties.Add("Updated", utcOffsetDate.ToString("O", CultureInfo.InvariantCulture));
                binder.Bind();
               var result= binder.UnBind();

                result.LocalCreated.Should().Be(localDate.ToUniversalTime(),because: "Only UTC date could be stored properly without local offset mismatch");
                result.LocalUpdated.Should().Be(utcDate);
                result.Created.Should().Be(localOffsetDate);
                result.Updated.Should().Be(utcOffsetDate);

            }
            finally
            {
                await client.DeleteAsync();
            }
        }

        [TestMethod]
        public async Task Should_Bind_Nullable_Types()
        {
            var partitionName = Guid.NewGuid().ToString();
            var person = Fakers.CreateFakePerson().Generate();

            var client = new TableClient(TestEnvironment.ConnectionString, NewTableName());
            try
            {
                await client.CreateIfNotExistsAsync();

                person.Altitude = null;
                person.Distance = default;
                person.Created = null;
                person.Situation = null;

                var binder = new TableEntityBinder<PersonEntity>(person, partitionName, person.PersonId.ToString());

                await client.UpsertEntityAsync(binder.Bind());
                var created = await client.GetEntityAsync<TableEntity>(binder.PartitionKey, binder.RowKey);

                var createdEntity = new TableEntityBinder<PersonEntity>(created).UnBind();

                createdEntity.Altitude.Should().Be(person.Altitude);
                createdEntity?.Distance.Should().Be(person.Distance);
            }
            finally
            {
                await client.DeleteAsync();
            }
        }

        private static async Task<TableEntity> MergeThenRetrieveAsync<T>(TableClient client, T tableEntity)
            where T : class, ITableEntity, new()

        {
            await client.UpsertEntityAsync(tableEntity, TableUpdateMode.Merge);
            return await client.GetEntityAsync<TableEntity>(tableEntity.PartitionKey, tableEntity.RowKey);
        }

        private static async Task<TableEntity> UpsertAndGetEntity<T>(TableClient client, T tableEntity)
           where T : class, ITableEntity, new()
        {
            await client.UpsertEntityAsync(tableEntity, TableUpdateMode.Replace);
            return await client.GetEntityAsync<TableEntity>(tableEntity.PartitionKey, tableEntity.RowKey);
        }

        private static string NewTableName() => $"{nameof(EntityBinderTests)}{Guid.NewGuid():N}".ToLowerInvariant();
    }
}