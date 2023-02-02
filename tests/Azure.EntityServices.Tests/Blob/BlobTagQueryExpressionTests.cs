using Azure.EntityServices.Blobs;
using Azure.EntityServices.Queries;
using Common.Samples.Models;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Linq;
using System.Reflection;

namespace Azure.EntityServices.Blob.Tests
{
    [TestClass]
    public class BlobTagQueryExpressionTests
    {
        [TestMethod]
        public void Should_Build_Query_Expression_With_Default_Instructions()
        {
            var builder = new BlobTagQueryBuilder<PersonEntity>();
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
        public void Should_Throw_Exception_Where_Instruction_Not_Implemented()
        {
            var builder = new BlobTagQueryBuilder<PersonEntity>();
            builder.Query

             .Where(p => p.Address.City)
             .Equal("Tokyo")
             //Invalid expression Blob TaG Query doesn't handler Not statement
             .AndNot(p => p.Where(p=>p.Enabled).Equal(true));
            Action builderAction = () => builder.Build();

            builderAction.Should().Throw<NotSupportedException>();
        }

        [TestMethod]
        public void Should_Throw_Exception_Where_Filter_Argument_WasNot_Only_A_Property_Selector()
        {
            var builder = new BlobTagQueryBuilder<PersonEntity>();

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
            var builder = new BlobTagQueryBuilder<PersonEntity>();
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

            result.Should().Be("\"Rowkey\" = '$Id-%+c5JcwURUajaem4NtAapw' AND \"City\" <> 'Paris' AND \"Created\" > '2012-04-21T18:25:43.0000000+00:00' AND \"Created\" > '2012-04-21T18:25:43.0000000+00:00' AND \"_MoreThanOneAddress\" > '2012-04-21T18:25:43.0000000+00:00' AND \"Enabled\" <> 'True'");
        }

        [TestMethod]
        public void Should_BuildGroup_Query_Expression_With_DefaultInstructions()
        {
            var builder = new BlobTagQueryBuilder<PersonEntity>();

            builder.Query
           .Where(p => p.TenantId).Equal("10")
            .And(p => p
                .Where(p => p.Created).GreaterThan(DateTimeOffset.Parse("2012-04-21T18:25:43Z"))
                .And(p => p.LastName).Equal("test")
                .Or(p => p.Created).LessThan(DateTimeOffset.Parse("2012-04-21T18:25:43Z")))
            .And(p => p
                    .Where(p => p.Created).GreaterThan(DateTimeOffset.Parse("2012-04-21T18:25:43Z"))
                    .Or(p => p.Created).LessThan(DateTimeOffset.Parse("2012-04-21T18:25:43Z")));
            var queryStr = builder.Build();

            queryStr.Trim()
                .Should()
                .Be("\"TenantId\" = '10' AND (\"Created\" > '2012-04-21T18:25:43.0000000+00:00' AND \"LastName\" = 'test' OR \"Created\" < '2012-04-21T18:25:43.0000000+00:00') AND (\"Created\" > '2012-04-21T18:25:43.0000000+00:00' OR \"Created\" < '2012-04-21T18:25:43.0000000+00:00')");
        }

        [TestMethod]
        public void Should_Build_TableStorage_Query_Expression()
        {
            var builder = new BlobTagQueryBuilder<PersonEntity>(new FilterExpression<PersonEntity>());

            builder.Query
           .Where("PartitionKey").Equal("Tenant-1")
           .And(p => p.TenantId).Equal("10")
           .And(p => p
              .Where(p => p.Created).GreaterThan(DateTimeOffset.Parse("2012-04-21T18:25:43Z"))
              .Or(p => p.Created).LessThan(DateTimeOffset.Parse("2012-04-21T18:25:43Z")));
            var queryStr = builder.Build();

            queryStr.Trim()
                .Should()
                .Be("\"PartitionKey\" = 'Tenant-1' AND \"TenantId\" = '10' AND (\"Created\" > '2012-04-21T18:25:43.0000000+00:00' OR \"Created\" < '2012-04-21T18:25:43.0000000+00:00')");
        }

        [TestMethod]
        public void Should_Build_Table_Storage_Advanced_Query_Expression()
        {
            var builder = new BlobTagQueryBuilder<PersonEntity>(new FilterExpression<PersonEntity>());

            builder.Query
           .Where("PartitionKey").Equal("Tenant-1")
           .And(p => p.TenantId).Equal("10")
           .And(p => p.Genre).Equal(Genre.Female);

            var queryStr = builder.Build();

            queryStr.Trim()
                .Should()
                .Be("\"PartitionKey\" = 'Tenant-1' AND \"TenantId\" = '10' AND \"Genre\" = 'Female'");
        }
    }
}