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
using System.Threading.Tasks;

namespace Azure.EntityServices.Table.Tests
{
    [TestClass]
    public class EntityTableClientQueryingTests
    {
        private readonly Action<EntityTableClientOptions> _defaultOptions;

        public EntityTableClientQueryingTests()
        {
            _defaultOptions = EntityTableClientCommon.DefaultOptions(nameof(EntityTableClientQueryingTests));
        }

        [TestMethod]
        public async Task Should_Filter_Entities()
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

        //for now query with null values are not supported in Azure Table Storage
        //event if, it works in Storage Emulator
        //[TestMethod]
        public async Task Should_Filter_Entities_With_Nullable_Properties()
        {
            var persons = Fakers.CreateFakePerson().Generate(10);

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

        [TestMethod]
        public async Task Should_Query_By_Tag_Filter_With_Equal_Extension()
        {
            var persons = Fakers.CreateFakePerson().Generate(10);

            var entityTable = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString).Configure(options => _defaultOptions(options), c =>
            {
                c.
                SetPartitionKey(p => p.TenantId)
                .SetRowKeyProp(p => p.PersonId)
                .AddTag(p => p.LastName)
                .AddTag(p => p.Created);
            });

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

        [TestMethod]
        public async Task Should_Query_By_Tag_Filter_With_GreaterThanOrEqual()
        {
            var persons = Fakers.CreateFakePerson().Generate(10);

            var entityTable = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString).Configure(options => _defaultOptions(options), c =>
            {
                c.
                SetPartitionKey(p => p.TenantId)
                .SetRowKeyProp(p => p.PersonId)
                .AddTag(p => p.LastName)
                .AddTag(p => p.Created);
            });

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

        [TestMethod]
        public async Task Should_Query_By_Tag_Filter_With_GreaterThan()
        {
            var persons = Fakers.CreateFakePerson().Generate(10);

            var entityTable = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString).Configure(options => _defaultOptions(options), c =>
            {
                c.
                SetPartitionKey(p => p.TenantId)
                .SetRowKeyProp(p => p.PersonId)
                .AddTag(p => p.LastName)
                .AddTag(p => p.Created);
            });

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

        [TestMethod]
        public async Task Should_Query_By_Tag_Filter_With_LessThan()
        {
            var persons = Fakers.CreateFakePerson().Generate(10);

            var entityTable = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString).Configure(options => _defaultOptions(options), c =>
            {
                c.
                SetPartitionKey(p => p.TenantId)
                .SetRowKeyProp(p => p.PersonId)
                .AddTag(p => p.LastName)
                .AddTag(p => p.Created);
            });

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

        [TestMethod]
        public async Task Should_Query_By_Tag_Filter_With_LessThanOrEqual()
        {
            var persons = Fakers.CreateFakePerson().Generate(10);

            var entityTable = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString).Configure(options => _defaultOptions(options), c =>
            {
                c.
                SetPartitionKey(p => p.TenantId)
                .SetRowKeyProp(p => p.PersonId)
                .AddTag(p => p.LastName)
                .AddTag(p => p.Created);
            });

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

        [TestMethod]
        public async Task Should_Query_By_Tag_Filter_With_Between_Extension()
        {
            var persons = Fakers.CreateFakePerson().Generate(10);

            var entityTable = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString).Configure(options => _defaultOptions(options), c =>
            {
                c.
                SetPartitionKey(p => p.TenantId)
                .SetRowKeyProp(p => p.PersonId)
                .AddTag(p => p.LastName)
                .AddTag(p => p.Created);
            });

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

        [TestMethod]
        public async Task Should_Combine_Query_With_Mixed_Tag_And_Prop_Filters()
        {
            var persons = Fakers.CreateFakePerson().Generate(10);

            var entityTable = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString).Configure(options => _defaultOptions(options), c =>
            {
                c.
                SetPartitionKey(p => p.TenantId)
                .SetRowKeyProp(p => p.PersonId)
                .AddTag(p => p.LastName)
                .AddTag(p => p.Created);
            });

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

        [TestMethod]
        public async Task Should_Query_By_Tag_Filter_With_Nullable_Values()
        {
            var persons = Fakers.CreateFakePerson().Generate(10);

            var entityTable = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString).Configure(options => _defaultOptions(options), c =>
            {
                c.
                SetPartitionKey(p => p.TenantId)
                .SetRowKeyProp(p => p.PersonId)
                .AddTag(p => p.LastName)
                .AddTag(p => p.Created);
            });

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

        [TestMethod]
        public async Task Should_Query_By_Tag_Filter_With_Empty_Values()
        {
            var persons = Fakers.CreateFakePerson().Generate(1);

            var entityTable = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString).Configure(options => _defaultOptions(options), c =>
            {
                c.
                SetPartitionKey(p => p.TenantId)
                .SetRowKeyProp(p => p.PersonId)
                .AddTag(p => p.LastName)
                .AddTag(p => p.Created);
            });

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

        [TestMethod]
        public async Task Should_Get_Paged_Entities()
        {
            var persons = Fakers.CreateFakePerson().Generate(155);
            var options = new EntityTableClientOptions() { };
            _defaultOptions.Invoke(options);

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

            await personClient.AddManyAsync(persons);

            EntityPage<PersonEntity> page = default;
            int entityCount = 0;

            do
            {
                page = await personClient.GetPagedAsync(nextPageToken: page.ContinuationToken);

                entityCount += page.Entities.Count();
            }
            while (!page.IsLastPage);

            entityCount.Should().Be(155);
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
            _defaultOptions.Invoke(options);

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

            await personClient.AddManyAsync(persons);

            EntityPage<PersonEntity> page = default;
            int entityCount = 0;

            do
            {
                page = await personClient.GetPagedAsync(maxPerPage: maxPerPage, iteratedCount: page.IteratedCount, nextPageToken: page.ContinuationToken);

                if (page.IsLastPage)
                {
                    page.Entities.Count().Should().BeLessThanOrEqualTo(maxPerPage);
                }
                else
                {
                    page.Entities.Count().Should().Be(maxPerPage);
                }
                entityCount += page.Entities.Count();
            }
            while (!page.IsLastPage);

            entityCount.Should().Be(totalCount);
            page.IteratedCount.Should().Be(entityCount);
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
            _defaultOptions.Invoke(options);

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

            await personClient.AddManyAsync(persons);

            var all = await personClient.GetAsync().ToListAsync();

            var skipResult = await personClient
                .GetAsync()
                .SkipAsync(skipCount)
                .ToListAsync();

            skipResult.Count.Should().Be(Math.Max(0, totalCount - skipCount));
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
            _defaultOptions.Invoke(options);

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

            await personClient.AddManyAsync(persons);

            var all = await personClient.GetAsync().ToListAsync();

            var takeResult = await personClient
                .GetAsync()
                .TakeAsync(takeCount)
                .ToListAsync();

            takeResult.Count.Should().Be(Math.Min(totalCount, takeCount));
        }

        [TestMethod]
        public async Task Should_Encode_Not_Supported_Chars_In_Query()
        {
            const string lastName = "O'Con/nor Mac'Leod?:+=,$&@";
            var persons = Fakers.CreateFakePerson().Generate(10);
            persons.Last().LastName = lastName;

            var entityTable = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString).Configure(options => _defaultOptions(options), c =>
            {
                c.
                SetPartitionKey(p => p.TenantId)
                .SetRowKeyProp(p => p.PersonId)
                .AddTag(p => p.LastName)
                .AddTag(p => p.Created);
            });
            await entityTable.AddManyAsync(persons);

            await foreach (var resultPage in entityTable.GetAsync(
                filter => filter
                .Where(p => p.LastName).Equal(lastName)))
            {
                resultPage.Count().Should().Be(1);
                resultPage.First().Should().BeEquivalentTo(persons.Last());
            }
        }


        [TestMethod]
        public async Task Should_Get_By_Indexed_Tag_With_Filter()
        {
            var persons = Fakers.CreateFakePerson().Generate(10);

            var entityTable = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString).Configure(options => _defaultOptions(options), c =>
            {
                c.
                SetPartitionKey(p => p.TenantId)
                .SetRowKeyProp(p => p.PersonId)
                .AddTag(p => p.LastName)
                .AddTag(p => p.Created);
            });

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

        [TestMethod]
        public async Task Should_Get_By_Indexed_Tag_Without_Given_Partition_Key()
        {
            var persons = Fakers.CreateFakePerson().Generate(10);

            var entityTable = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString).Configure(options => _defaultOptions(options), c =>
            {
                c.
                SetPartitionKey(p => p.TenantId)
                .SetRowKeyProp(p => p.PersonId)
                .AddTag(p => p.LastName)
                .AddTag(p => p.Created);
            });

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

        [TestMethod]
        public async Task Should_Get_Indexed_Tag_After_InsertOrUpdate()
        {
            var person = Fakers.CreateFakePerson().Generate();
            var tableEntity = EntityTableClient.Create<PersonEntity>(TestEnvironment.ConnectionString).Configure(options => _defaultOptions(options), c =>
            {
                c.SetPartitionKey(p => p.TenantId);
                c.SetRowKeyProp(p => p.PersonId);
                c.AddTag(p => p.LastName);
            });

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


    }
}