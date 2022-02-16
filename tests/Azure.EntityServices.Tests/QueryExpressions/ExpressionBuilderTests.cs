using Azure.EntityServices.Queries;
using Azure.EntityServices.Tables.Core;
using Azure.EntityServices.Table.Common.Models;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Reflection;
using Azure.EntityServices.Tables.Extensions;

namespace Azure.EntityServices.Table.Tests
{
    [TestClass]
    public class QueryExpressionTests
    {
        [TestMethod]
        public void Should_Build_Query_Expression_With_Default_Instructions()
        {
            var builder = new MockedExpressionBuilder<PersonEntity>();
            builder
            .Query
                .Where(p => p.Rank).Equal(10)
                .And(p => p.Address.City).NotEqual("Paris")
                .And(p => p.Created).GreaterThan(DateTimeOffset.UtcNow)
                .And(p => p.Enabled).NotEqual(true);

            builder.Query.NextOperation.Operator.Should().Be("And");
            var result = builder.Build();
            result.Should().NotBeNullOrEmpty();
        }

        [TestMethod]
        public void Should_Throw_Exception_Where_Filter_Argument_WasNot_Only_A_Property_Selector()
        {
            var builder = new MockedExpressionBuilder<PersonEntity>();

            Action builderAction = () => builder.Query

             .Where(p => p.Address.City).NotEqual("Tokyo")

             //Invalid expression , should be a simple prop selector like bellow
             .And(p => p.Address.City != "Paris");

            builderAction.Should().Throw<InvalidFilterCriteriaException>()
            .WithMessage("Given Expression should be a valid property selector");
        }

        [TestMethod]
        public void Should_Build_Mixed_Query_Selector_Expression_With_Default_Instructions()
        {
            var builder = new MockedExpressionBuilder<PersonEntity>();
            builder.Query
            //RowKey is a native prop of Azyre storage ITableEntiy
            .Where("Rowkey").Equal("$Id-%+c5JcwURUajaem4NtAapw")
            .And(p => p.Address.City).NotEqual("Paris")

            //Created is an entity prop wish could be requested by string or prop selector
            .And("Created").GreaterThan(DateTimeOffset.Parse("2012-04-21T18:25:43Z"))
            .And(p => p.Created).GreaterThan(DateTimeOffset.Parse("2012-04-21T18:25:43Z"))

            //_MoreThanOneAddress is a dynamic prop
            .And("_MoreThanOneAddress").GreaterThan(DateTimeOffset.Parse("2012-04-21T18:25:43Z"))
            .And(p => p.Enabled).NotEqual(true);
            var result = builder.Build();

            result.Should().Be("Rowkey Equal '$Id-%+c5JcwURUajaem4NtAapw' And City NotEqual 'Paris' And Created GreaterThan '2012-04-21T18:25:43.0000000+00:00' And Created GreaterThan '2012-04-21T18:25:43.0000000+00:00' And _MoreThanOneAddress GreaterThan '2012-04-21T18:25:43.0000000+00:00' And Enabled NotEqual 'True'");
        }

        [TestMethod]
        public void Should_BuildGroup_Query_Expression_With_DefaultInstructions()
        {
            var builder = new MockedExpressionBuilder<PersonEntity>();

            builder.Query
           .Where(p => p.TenantId).Equal("10")
            .And(p => p
                .Where(p => p.Created).GreaterThan(DateTimeOffset.Parse("2012-04-21T18:25:43Z"))
                .And(p => p.LastName).Equal("test")
                .Or(p => p.Created).LessThan(DateTimeOffset.Parse("2012-04-21T18:25:43Z")))
            .Not(p => p.Enabled).Equal(true)
            .And(p => p
                    .Where(p => p.Created).GreaterThan(DateTimeOffset.Parse("2012-04-21T18:25:43Z"))
                    .Or(p => p.Created).LessThan(DateTimeOffset.Parse("2012-04-21T18:25:43Z")));
            var queryStr = builder.Build();

            queryStr.Trim()
                .Should()
                .Be("TenantId Equal '10' And (Created GreaterThan '2012-04-21T18:25:43.0000000+00:00' And LastName Equal 'test' Or Created LessThan '2012-04-21T18:25:43.0000000+00:00') Not Enabled Equal 'True' And (Created GreaterThan '2012-04-21T18:25:43.0000000+00:00' Or Created LessThan '2012-04-21T18:25:43.0000000+00:00')");
        }

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
            var builder = new TableStorageQueryBuilder<PersonEntity>(new TagFilterExpression<PersonEntity>("Created",(k,v)=> $"{k}-{v}"));

            (builder.Query as TagFilterExpression<PersonEntity>)
           .WhereTag().Equal("2022-10-22")
           .And(p => p.TenantId).Equal("10");

            var queryStr = builder.Build();

            queryStr.Trim()
                .Should()
                .Be("RowKey gt 'Created-2022-10-22' and RowKey lt 'Created-2022-10-22~' and _deleted_tag_ eq false and TenantId eq '10'");

        }
        [TestMethod]
        public void Should_Use_Tag_GreaterThanOrEqual_Extension()
        {
            var builder = new TableStorageQueryBuilder<PersonEntity>(new TagFilterExpression<PersonEntity>("Created",(k,v)=> $"{k}-{v}"));

            (builder.Query as TagFilterExpression<PersonEntity>)
           .WhereTag().GreaterThanOrEqual("2022-10-22")
           .And(p => p.TenantId).Equal("10");

            var queryStr = builder.Build();

            queryStr.Trim()
                .Should()
                .Be("RowKey gt 'Created-2022-10-22' and RowKey lt 'Created-~' and _deleted_tag_ eq false and TenantId eq '10'");

        }
        [TestMethod]
        public void Should_Use_Tag_LessThanOrEqual_Extension()
        {
            var builder = new TableStorageQueryBuilder<PersonEntity>(new TagFilterExpression<PersonEntity>("Created",(k, v) => $"{k}-{v}"));

            (builder.Query as TagFilterExpression<PersonEntity>)
           .WhereTag().LessThanOrEqual("2022-10-22")
           .And(p => p.TenantId).Equal("10");

            var queryStr = builder.Build();

            queryStr.Trim()
                .Should()
                .Be("RowKey gt 'Created-' and RowKey lt 'Created-2022-10-22~' and _deleted_tag_ eq false and TenantId eq '10'");

        }

    }
}