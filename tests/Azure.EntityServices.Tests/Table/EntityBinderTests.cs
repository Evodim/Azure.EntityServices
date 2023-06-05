using Azure.Data.Tables;
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

            var tableEntity = new TableEntityAdapter<PersonEntity>(person, partitionName, person.PersonId.ToString());
            var client = new Data.Tables.TableClient(TestEnvironment.ConnectionString, NewTableName());
            try
            {
                await client.CreateIfNotExistsAsync();

                var result = await UpsertAndGetEntity(client, tableEntity.WriteToEntityModel());

                var adapterResult = new TableEntityAdapter<PersonEntity>(result);

                var entity = adapterResult.ReadFromEntityModel();

                entity.Altitude.Should().Be(person.Altitude);
                entity.BankAmount.Should().Be(person.BankAmount);
                entity.Situation.Should().Be(person.Situation);
            }
            catch { throw; }
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

            var adapter = new TableEntityAdapter<PersonEntity>(person, partitionName, person.PersonId.ToString());

            var client = new Data.Tables.TableClient(TestEnvironment.ConnectionString, NewTableName());
            try
            {
                await client.CreateIfNotExistsAsync();

                await UpsertAndGetEntity(client, adapter.WriteToEntityModel());
                adapter = new TableEntityAdapter<PersonEntity>(new PersonEntity()
                {
                    PersonId = person.PersonId,
                    FirstName = "John Do",
                    LocalCreated = null,
                    LocalUpdated = default,
                    Updated = default,
                    Enabled = false
                }, partitionName,

                person.PersonId.ToString());

                var merged = await MergeThenRetrieveAsync(client, adapter.WriteToEntityModel());
                var adapterResult = new TableEntityAdapter<PersonEntity>(merged);
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
            catch { throw; }
            finally
            {
                await client.DeleteAsync();
            }
        }

        [TestMethod]
        public async Task Should_Bind_On_Update()
        {
            var client = new Data.Tables.TableClient(TestEnvironment.ConnectionString, NewTableName());
            try
            {
                await client.CreateIfNotExistsAsync();

                var partitionName = Guid.NewGuid().ToString();

                var person = Fakers.CreateFakePerson().Generate();
                var adapter = new TableEntityAdapter<PersonEntity>(person, partitionName, person.PersonId.ToString());

                var replaced = await UpsertAndGetEntity(client, adapter.WriteToEntityModel());
                var adapterResult = new TableEntityAdapter<PersonEntity>(replaced);

                adapterResult.ReadFromEntityModel();

                adapterResult.RowKey.Should().Be(person.PersonId.ToString());
                adapterResult.Entity.Should().BeEquivalentTo(person);
            }
            catch { throw; }
            finally
            {
                await client.DeleteAsync();
            }
        }

        [TestMethod]
        public async Task Should_Bind_Metadatas_On_Update()
        {
            var client = new Data.Tables.TableClient(TestEnvironment.ConnectionString, NewTableName());
            try
            {
                await client.CreateIfNotExistsAsync();

                var partitionName = Guid.NewGuid().ToString();
                var person = Fakers.CreateFakePerson().Generate();

                var adapter = new TableEntityAdapter<PersonEntity>(person, partitionName, person.PersonId.ToString());
                adapter.Metadata.Add("_HasChildren", true);
                adapter.Metadata.Add("_Deleted", false);

                await UpsertAndGetEntity(client, adapter.WriteToEntityModel());
                adapter = new TableEntityAdapter<PersonEntity>(person, partitionName, person.PersonId.ToString());
                adapter.Metadata.Add("_HasChildren", false);

                var replaced = await UpsertAndGetEntity(client, adapter.WriteToEntityModel());
                var adapterResult = new TableEntityAdapter<PersonEntity>(replaced);
                adapterResult.ReadFromEntityModel();

                adapterResult.Entity.Should().BeEquivalentTo(person);
                adapterResult.Metadata.Should().Contain("_HasChildren", false);
                adapterResult.Metadata.Should().NotContainKey("_Deleted", because: "InsertOrReplace replace all entity props and it's metadatas");
            }
            catch { throw; }
            finally
            {
                await client.DeleteAsync();
            }
        }

        [TestMethod]
        public async Task Should_Bind_Metadatas_On_Merge()
        {
            var client = new Data.Tables.TableClient(TestEnvironment.ConnectionString, NewTableName());
            try
            {
                await client.CreateIfNotExistsAsync();

                var partitionName = Guid.NewGuid().ToString();

                var person = Fakers.CreateFakePerson().Generate();

                var adapter = new TableEntityAdapter<PersonEntity>(person, partitionName, person.PersonId.ToString());
                adapter.Metadata.Add("_HasChildren", true);
                adapter.Metadata.Add("_Deleted", true);
                adapter.WriteToEntityModel();

                await UpsertAndGetEntity(client, adapter.WriteToEntityModel());

                adapter = new TableEntityAdapter<PersonEntity>(person, partitionName, person.PersonId.ToString());
                adapter.Metadata.Add("_HasChildren", false);

                var merged = await MergeThenRetrieveAsync(client, adapter.WriteToEntityModel());
                var adapterResult = new TableEntityAdapter<PersonEntity>(merged);

                adapterResult.ReadFromEntityModel();

                adapterResult.Entity.Should().BeEquivalentTo(person);
                adapterResult.Metadata.Should().Contain("_HasChildren", false);
                adapterResult.Metadata.Should().ContainKey("_Deleted");
                adapterResult.Metadata.Should().Contain("_Deleted", true);
            }
            catch { throw; }
            finally
            {
                await client.DeleteAsync();
            }
        }

        [TestMethod]
        public async Task Should_Bind_DynamicProps()
        {
            var client = new Data.Tables.TableClient(TestEnvironment.ConnectionString, NewTableName());
            try
            {
                var dynamicProps = new Dictionary<string, Func<PersonEntity, object>>() { ["_distance_less_than_500m"] = (e) => e.Distance < 500 };
                await client.CreateIfNotExistsAsync();

                var partitionName = Guid.NewGuid().ToString();
                var person = Fakers.CreateFakePerson().Generate();

                person.Distance = 250;
                var adapter = new TableEntityAdapter<PersonEntity>(person, partitionName, person.PersonId.ToString());
                adapter.BindDynamicProps(dynamicProps);

                var added = await UpsertAndGetEntity(client, adapter.WriteToEntityModel());
                added.Should().ContainKey("_distance_less_than_500m");
                (added["_distance_less_than_500m"] as bool?)?.Should().BeTrue();

                person.Distance = 501;
                var adapterToUpdate = new TableEntityAdapter<PersonEntity>(person, partitionName, person.PersonId.ToString());
                adapterToUpdate.BindDynamicProps(dynamicProps);

                var replaced = await UpsertAndGetEntity(client, adapterToUpdate.WriteToEntityModel());

                replaced.Should().ContainKey("_distance_less_than_500m");
                (replaced["_distance_less_than_500m"] as bool?)?.Should().BeFalse();
            }
            catch
            {
                throw;
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

            var client = new Data.Tables.TableClient(TestEnvironment.ConnectionString, NewTableName());
            try
            {
                await client.CreateIfNotExistsAsync();
                var localDate = DateTime.Now;
                var utcDate = DateTime.UtcNow;
                var localOffsetDate = DateTimeOffset.Now;
                var utcOffsetDate = DateTimeOffset.UtcNow;

                var adapter = new TableEntityAdapter<PersonEntity>(person, partitionName, person.PersonId.ToString());
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
            catch
            {
                throw;
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

            var client = new Data.Tables.TableClient(TestEnvironment.ConnectionString, NewTableName());
            try
            {
                await client.CreateIfNotExistsAsync();

                person.Altitude = null;
                person.Distance = default;
                person.Created = null;
                person.Situation = null;

                var adapter = new TableEntityAdapter<PersonEntity>(person, partitionName, person.PersonId.ToString());

                await client.UpsertEntityAsync(adapter.WriteToEntityModel());
                var created = await client.GetEntityAsync<TableEntity>(adapter.PartitionKey, adapter.RowKey);

                var createdEntity = new TableEntityAdapter<PersonEntity>(created).ReadFromEntityModel();

                createdEntity.Altitude.Should().Be(person.Altitude);
                createdEntity?.Distance.Should().Be(person.Distance);
            }
            catch { throw; }
            finally
            {
                await client.DeleteAsync();
            }
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

        private static string NewTableName() => $"{nameof(EntityBinderTests)}{Guid.NewGuid():N}".ToLowerInvariant();
    }
}