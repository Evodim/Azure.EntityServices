﻿using Azure.EntityServices.Queries;
using Azure.EntityServices.Tables;
using Azure.EntityServices.Tables.Core.Implementations;
using Common.Samples.Models;
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
            var builder = new AzureTableStorageQueryBuilder<PersonEntity>(new TagFilterExpression<PersonEntity>());

            builder.Query
           .Where("PartitionKey").Equal("Tenant-1")
           .And(p => p.TenantId).Equal("10")
           .And(p => p
              .Where(p => p.Created).GreaterThan(DateTimeOffset.Parse("2012-04-21T18:25:43Z"))
              .Or(p => p.Created).LessThan(DateTimeOffset.Parse("2012-04-21T18:25:43Z"))
            )
           .And(p => p.Enabled).Equal(true);

            var queryStr = builder.Build();

            queryStr.Trim()
                .Should()
                .Be("PartitionKey eq 'Tenant-1' and TenantId eq '10' and (Created gt datetime'2012-04-21T18:25:43.0000000Z' or Created lt datetime'2012-04-21T18:25:43.0000000Z') and Enabled eq true");
        }

        [TestMethod]
        public void Should_Build_Table_Storage_Advanced_Query_Expression()
        {
            var builder = new AzureTableStorageQueryBuilder<PersonEntity>(new TagFilterExpression<PersonEntity>());

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
            var builder = new AzureTableStorageQueryBuilder<PersonEntity>(new TagFilterExpression<PersonEntity>());

            (builder.Query as TagFilterExpression<PersonEntity>)
           .WhereTag("Created").Equal("2022-10-22")
           .And(p => p.TenantId).Equal("10");

            var queryStr = builder.Build();

            queryStr.Trim()
                .Should()
                .Be("RowKey gt '~Created-2022-10-22$' and RowKey lt '~Created-2022-10-22$~' and TenantId eq '10'");
        }

        [TestMethod]
        public void Should_Use_Tag_GreaterThanOrEqual_Extension()
        {
            var builder = new AzureTableStorageQueryBuilder<PersonEntity>(new TagFilterExpression<PersonEntity>());

            (builder.Query as TagFilterExpression<PersonEntity>)
           .WhereTag("Created").GreaterThanOrEqual("2022-10-22")
           .And(p => p.TenantId).Equal("10");

            var queryStr = builder.Build();

            queryStr.Trim()
                .Should()
                .Be("RowKey gt '~Created-2022-10-22$' and RowKey lt '~Created-~' and TenantId eq '10'");
        }

        [TestMethod]
        public void Should_Use_Tag_LessThanOrEqual_Extension()
        {
            var builder = new AzureTableStorageQueryBuilder<PersonEntity>(new TagFilterExpression<PersonEntity>());

            (builder.Query as TagFilterExpression<PersonEntity>)
           .WhereTag("Created").LessThanOrEqual("2022-10-22")
           .And(p => p.TenantId).Equal("10");

            var queryStr = builder.Build();

            queryStr.Trim()
                .Should()
                .Be("RowKey gt '~Created-' and RowKey lt '~Created-2022-10-22$~' and TenantId eq '10'");
        }

        [TestMethod]
        public void Should_Use_filter_with_null_values()
        {
            var builder = new AzureTableStorageQueryBuilder<PersonEntity>(new TagFilterExpression<PersonEntity>());

            (builder.Query as TagFilterExpression<PersonEntity>)
           .WhereTag("Created").Equal(null)
           .And(p => p.TenantId).Equal("10")
           .And(p => p.Created).Equal(null)
           .Or(p => p.Enabled).Equal(null)
           .Or(p => p.Altitude).Equal(null);

            var queryStr = builder.Build();

            queryStr.Trim()
                .Should()
                .Be("RowKey gt '~Created-$' and RowKey lt '~Created-$~' and TenantId eq '10' and Created eq null or Enabled eq null or Altitude eq null");
        }

        [TestMethod]
        public void Should_Use_filter_with_default_values()
        {
            var builder = new AzureTableStorageQueryBuilder<PersonEntity>(new TagFilterExpression<PersonEntity>());

            (builder.Query as TagFilterExpression<PersonEntity>)
           .WhereTag("Created").Equal(default)
           .And(p => p.TenantId).Equal("10")
           .And(p => p.Updated).Equal(default)
           .And(p => p.LocalUpdated).Equal(default)
           .Or(p => p.Enabled).Equal(default)
           .Or(p => p.Altitude).Equal(default);

            var queryStr = builder.Build();

            queryStr.Trim()
                .Should()
                .Be("RowKey gt '~Created-$' and RowKey lt '~Created-$~' and TenantId eq '10' and Updated eq datetime'1601-01-01T00:00:00.0000000Z' and LocalUpdated eq datetime'1601-01-01T00:00:00.0000000Z' or Enabled eq null or Altitude eq null");
        }

        [TestMethod]
        public void Should_Build_Table_Filter_Expression_With_Invariant_Floating_Point_Value_Filter()
        {
            var builder = new AzureTableStorageQueryBuilder<PersonEntity>(new FilterExpression<PersonEntity>());
            builder
            .Query
                .WherePartitionKey().Equal("tenantId")
                .And(p => p.Latitude).Equal(48.77309806265856)
                .And(p => p.Distance).Equal(148.45648566856M)
                .And(p => p.BankAmount).Equal(1248.7731F);

            var result = builder.Build();
            result.Should()
            .Be("PartitionKey eq 'tenantId' and Latitude eq 48.77309806265856 and Distance eq '148.45648566856' and BankAmount eq '1248.7731'");
        }

        [TestMethod]
        public void Should_Use_Multi_PartitionKey_filter()
        {
            var builder = new AzureTableStorageQueryBuilder<PersonEntity>(new TagFilterExpression<PersonEntity>("Created"));

            (builder.Query as TagFilterExpression<PersonEntity>)
           .WherePartitionKey().Equal("partition1")
           .OrPartitionKey().Equal("partition2")
           .OrPartitionKey().Equal("Partition3");

            var queryStr = builder.Build();

            queryStr.Trim()
                .Should()
                .Be("PartitionKey eq 'partition1' or PartitionKey eq 'partition2' or PartitionKey eq 'Partition3'");
        }

        [TestMethod]
        public void Should_Use_WithTag_Extension_To_Get_All_Tag_Values_Without_Filter_Operator()
        {
            var builder = new AzureTableStorageQueryBuilder<PersonEntity>(new TagFilterExpression<PersonEntity>());

            (builder.Query as TagFilterExpression<PersonEntity>)
           .WithTag("Created")
           .And(p => p.TenantId).Equal("10");
            var queryStr = builder.Build();

            queryStr.Trim()
                .Should()
                .Be("RowKey gt '~Created-' and RowKey lt '~Created-~' and TenantId eq '10'");
        }

        [TestMethod]
        public void Should_Use_IncludeTags_To_Get_All_Entities_Included_All_Tags()
        {
            var builder = new AzureTableStorageQueryBuilder<PersonEntity>(new TagFilterExpression<PersonEntity>());

            (builder.Query as TagFilterExpression<PersonEntity>)
           .IncludeTags()
           .WherePartitionKey().Equal("tenant1");

            var queryStr = builder.Build();

            queryStr.Trim()
                .Should()
                .Be("PartitionKey eq 'tenant1'");
        }

        [TestMethod]
        public void Should_Use_In_Filter_With_Tag_Prop()
        {
            var builder = new AzureTableStorageQueryBuilder<PersonEntity>(new TagFilterExpression<PersonEntity>());

            (builder.Query as TagFilterExpression<PersonEntity>)
           .Where("PartitionKey").In("value1", "value2", "value3");

            var queryStr = builder.Build();
            queryStr.Trim()
                .Should()
                .Be("(PartitionKey eq 'value1' or PartitionKey eq 'value2' or PartitionKey eq 'value3')");
        }

        [TestMethod]
        public void Should_Use_In_Filter_Inside_ExpressionFilter_Prop2()
        {
            var builder = new AzureTableStorageQueryBuilder<PersonEntity>(new TagFilterExpression<PersonEntity>());

            (builder.Query as TagFilterExpression<PersonEntity>)
           .Where("prop1s").Equal("value")
           .And("tenant1").In("tag1", "tag2", "tag3")
           .And("prop2").Equal("newValue");

            var queryStr = builder.Build();

            queryStr.Trim()
                .Should()
                .Be("prop1s eq 'value' and (tenant1 eq 'tag1' or tenant1 eq 'tag2' or tenant1 eq 'tag3') and prop2 eq 'newValue'");
        }

        [TestMethod]
        public void Should_Use_In_Filter_At_Start_Of_ExpressionFilter_Prop2()
        {
            var builder = new AzureTableStorageQueryBuilder<PersonEntity>(new TagFilterExpression<PersonEntity>());

            (builder.Query as TagFilterExpression<PersonEntity>)
           .Where("tenant1").In("tag1", "tag2", "tag3")
           .And("prop2").Equal("newValue");

            var queryStr = builder.Build();

            queryStr.Trim()
                .Should()
                .Be("(tenant1 eq 'tag1' or tenant1 eq 'tag2' or tenant1 eq 'tag3') and prop2 eq 'newValue'");
        }

        [TestMethod]
        public void Should_Use_NotIn_Filter_With_Tag_Prop()
        {
            var builder = new AzureTableStorageQueryBuilder<PersonEntity>(new TagFilterExpression<PersonEntity>());

            (builder.Query as TagFilterExpression<PersonEntity>)
           .Where("PartitionKey").NotIn("value1", "value2", "value3");

            var queryStr = builder.Build();
            queryStr.Trim()
                .Should()
                .Be("(PartitionKey ne 'value1' and PartitionKey ne 'value2' and PartitionKey ne 'value3')");
        }

        [TestMethod]
        public void Should_Use_NotIn_Filter_Inside_ExpressionFilter()
        {
            var builder = new AzureTableStorageQueryBuilder<PersonEntity>(new TagFilterExpression<PersonEntity>());

            (builder.Query as TagFilterExpression<PersonEntity>)
           .Where("prop1s").Equal("value")
           .And("tenant1").NotIn("tag1", "tag2", "tag3")
           .And("prop2").Equal("newValue");

            var queryStr = builder.Build();

            queryStr.Trim()
                .Should()
                .Be("prop1s eq 'value' and (tenant1 ne 'tag1' and tenant1 ne 'tag2' and tenant1 ne 'tag3') and prop2 eq 'newValue'");
        }

        [TestMethod]
        public void Should_Use_NotIn_Filter_At_Start_Of_ExpressionFilter()
        {
            var builder = new AzureTableStorageQueryBuilder<PersonEntity>(new TagFilterExpression<PersonEntity>());

            (builder.Query as TagFilterExpression<PersonEntity>)
           .Where("tenant1").NotIn("tag1", "tag2", "tag3")
           .And("prop2").Equal("newValue");

            var queryStr = builder.Build();

            queryStr.Trim()
                .Should()
                .Be("(tenant1 ne 'tag1' and tenant1 ne 'tag2' and tenant1 ne 'tag3') and prop2 eq 'newValue'");
        }

        [TestMethod]
        public void Should_Use_NotIn_Filter_At_End_Of_ExpressionFilter()
        {
            var builder = new AzureTableStorageQueryBuilder<PersonEntity>(new TagFilterExpression<PersonEntity>());

            (builder.Query as TagFilterExpression<PersonEntity>)
           .Where("tenant1").NotIn("tag1", "tag2", "tag3");
            var queryStr = builder.Build();
            queryStr.Trim()
                .Should()
                .Be("(tenant1 ne 'tag1' and tenant1 ne 'tag2' and tenant1 ne 'tag3')");
        }
    }
}