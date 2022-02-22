using Azure.EntityServices.Queries;
using Azure.EntityServices.Table.Common.Models;
using Azure.EntityServices.Tables;
using Azure.EntityServices.Tables.Core;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;

namespace Azure.EntityServices.Table.Tests
{
    [TestClass]
    public class TableStorageQueryBuilderTests
    {
        [TestMethod]
        public void Should_Build_TableStorage_Query_Expression()
        {
            var builder = new TableStorageQueryBuilder<PersonEntity>(new FilterExpression<PersonEntity>());

            builder.Query
           .Where("PartitionKey").Equal("Tenant-1")
           .And(p => p.TenantId).Equal("10")
           .And(p => p
              .Where(p => p.Created).GreaterThan(DateTimeOffset.Parse("2012-04-21T18:25:43Z"))
              .Or(p => p.Created).LessThan(DateTimeOffset.Parse("2012-04-21T18:25:43Z")))
           .Not(p => p.Enabled).Equal(true);

            var queryStr = builder.Build();

            queryStr.Trim()
                .Should()
                .Be("PartitionKey eq 'Tenant-1' and TenantId eq '10' and (Created gt datetime'2012-04-21T18:25:43.0000000Z' or Created lt datetime'2012-04-21T18:25:43.0000000Z') not Enabled eq true");
        }

        [TestMethod]
        public void Should_Build_Table_Storage_Advanced_Query_Expression()
        {
            var builder = new TableStorageQueryBuilder<PersonEntity>(new FilterExpression<PersonEntity>());

            builder.Query
           .Where("PartitionKey").Equal("Tenant-1")
           .And(p => p.TenantId).Equal("10")
           .And(p => p.Genre).Equal(Genre.Female);

            var queryStr = builder.Build();

            queryStr.Trim()
                .Should()
                .Be("PartitionKey eq 'Tenant-1' and TenantId eq '10' and Genre eq 'Female'");
        }

        [TestMethod]
        public void Should_Tag_Equal_Extension()
        {
            var builder = new TableStorageQueryBuilder<PersonEntity>(new TagFilterExpression<PersonEntity>("Created"));

            (builder.Query as TagFilterExpression<PersonEntity>)
           .WhereTag().Equal("2022-10-22")
           .And(p => p.TenantId).Equal("10");

            var queryStr = builder.Build();

            queryStr.Trim()
                .Should()
                .Be("RowKey gt 'Created-2022-10-22$' and RowKey lt 'Created-2022-10-22$~' and _deleted_tag_ eq false and TenantId eq '10'");
        }

        [TestMethod]
        public void Should_Use_Tag_GreaterThanOrEqual_Extension()
        {
            var builder = new TableStorageQueryBuilder<PersonEntity>(new TagFilterExpression<PersonEntity>("Created"));

            (builder.Query as TagFilterExpression<PersonEntity>)
           .WhereTag().GreaterThanOrEqual("2022-10-22")
           .And(p => p.TenantId).Equal("10");

            var queryStr = builder.Build();

            queryStr.Trim()
                .Should()
                .Be("RowKey gt 'Created-2022-10-22$' and RowKey lt 'Created-~' and _deleted_tag_ eq false and TenantId eq '10'");
        }

        [TestMethod]
        public void Should_Use_Tag_LessThanOrEqual_Extension()
        {
            var builder = new TableStorageQueryBuilder<PersonEntity>(new TagFilterExpression<PersonEntity>("Created"));

            (builder.Query as TagFilterExpression<PersonEntity>)
           .WhereTag().LessThanOrEqual("2022-10-22")
           .And(p => p.TenantId).Equal("10");

            var queryStr = builder.Build();

            queryStr.Trim()
                .Should()
                .Be("RowKey gt 'Created-' and RowKey lt 'Created-2022-10-22$~' and _deleted_tag_ eq false and TenantId eq '10'");
        }

        [TestMethod]
        public void Should_Use_Nullable_Filter()
        {
            var builder = new TableStorageQueryBuilder<PersonEntity>(new TagFilterExpression<PersonEntity>("Created"));

            (builder.Query as TagFilterExpression<PersonEntity>)
           .WhereTag().Equal(null)
           .And(p => p.TenantId).Equal("10")
           .And(p => p.Created).Equal(null)
           .Or(p => p.Enabled).Equal(null)
           .Or(p => p.Altitude).Equal(null);

            var queryStr = builder.Build();

            queryStr.Trim()
                .Should()
                .Be("RowKey gt 'Created-$null$' and RowKey lt 'Created-$null$~' and _deleted_tag_ eq false and TenantId eq '10' and Created eq null or Enabled eq null or Altitude eq null");
        }
    }
}