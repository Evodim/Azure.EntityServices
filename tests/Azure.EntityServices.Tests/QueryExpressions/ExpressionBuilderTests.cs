using Azure.EntityServices.Queries;
using Azure.EntityServices.Table.Common.Models;
using Azure.EntityServices.Tables;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Reflection;

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
        public void Should_Build_Query_Expression_With_Typed_Filter()
        {
            var builder = new MockedExpressionBuilder<PersonEntity>();
            builder
            .Query
                .WherePartitionKey()
                .Equal("tenantId")
                .And(p => p.Altitude)
                .Equal(0.00M);

            var result = builder.Build();
            result.Should()
            .Be("PartitionKey Equal 'tenantId' And Altitude Equal '0.00'");
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
        public void Should_BuildGroup_Dynamic_Query_Expression()
        {
            var builder = new MockedExpressionBuilder<PersonEntity>();

            var dynamicQuery = builder.Query
            .Where(p => p.TenantId).Equal("50");
            for (var i = 0; i < 10; i++)
            {
                dynamicQuery = dynamicQuery.Or(p => p.FirstName).Equal($"do {i}");
            }

            var queryStr = builder.Build();

            queryStr.Trim()
                .Should()
                .Be("TenantId Equal '50' Or FirstName Equal 'do 0' Or FirstName Equal 'do 1' Or FirstName Equal 'do 2' Or FirstName Equal 'do 3' Or FirstName Equal 'do 4' Or FirstName Equal 'do 5' Or FirstName Equal 'do 6' Or FirstName Equal 'do 7' Or FirstName Equal 'do 8' Or FirstName Equal 'do 9'");
        }

        [TestMethod]
        public void Should_BuildGroup_Dynamic_Query_Expression_With_Extension_Helper()
        {
            var builder = new MockedExpressionBuilder<PersonEntity>();

            var dynamicQuery = builder.Query
              .Where(p => p.TenantId).Equal("50");

            dynamicQuery.WithEach(new int[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 }, (item, q) =>
                        q.Or(p => p.FirstName).Equal($"do {item}"));

            var queryStr = builder.Build();

            queryStr.Trim()
                .Should()
                .Be("TenantId Equal '50' Or FirstName Equal 'do 0' Or FirstName Equal 'do 1' Or FirstName Equal 'do 2' Or FirstName Equal 'do 3' Or FirstName Equal 'do 4' Or FirstName Equal 'do 5' Or FirstName Equal 'do 6' Or FirstName Equal 'do 7' Or FirstName Equal 'do 8' Or FirstName Equal 'do 9'");
        }
    }
}