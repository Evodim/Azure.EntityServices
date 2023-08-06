using Azure.EntityServices.Tables.Core;
using Bogus;
using Common.Samples.Models;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Linq;

namespace Azure.EntityServices.Tests.Table
{
    [TestClass]
    public class TableQueryHelperTests
    {
        [TestMethod]
        public void ShouldEncodeDisalowedValuesForPartitionKeyField()
        {
            var person = new Faker<PersonEntity>().Generate(1).First();
            person.LastName = "\u008fName used as key /\\#?Person123!\n\t\r\0\a\u009f1";

            var encodedKey = TableQueryHelper.ToPartitionKey(person.LastName); 
        
            encodedKey.Should().Be("*Name used as key ****Person123!******1");
        }
        [TestMethod]
        public void ShouldEncodeDisalowedValuesForPrimaryKeyField()
        {
            var person = new Faker<PersonEntity>().Generate(1).First();
            person.LastName = "\u008fName used as key /\\#?Person123!\n\t\r\0\a\u009f2";

            var encodedKey = TableQueryHelper.ToPrimaryRowKey(person.LastName);

            encodedKey.Should().Be("*Name used as key ****Person123!******2");
        }
        [TestMethod]
        public void ShouldEncodeDisalowedValuesForTagKeyField()
        {
            var person = new Faker<PersonEntity>().Generate(1).First();
            person.LastName = "\u008fName used as key /\\#?Person123!\n\t\r\0\a\u009f3";

            var encodedKey = TableQueryHelper.ToTagRowKeyPrefix("lastname", person.LastName);

            encodedKey.Should().Be("~lastname-*Name used as key ****Person123!******3$");
        }
    }
}