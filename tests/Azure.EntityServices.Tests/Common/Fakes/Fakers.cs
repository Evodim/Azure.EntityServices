using Azure.EntityServices.Table.Common.Models;
using Bogus;
using Bogus.Extensions;
using System;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

namespace Azure.EntityServices.Table.Common.Fakes
{
    public static class Fakers
    {
        private static Task<byte[]> DownloadImage(string uri)
        {
            using var client = new WebClient();
            return client.DownloadDataTaskAsync(new Uri(uri));
        }

        public static Faker<DocumentEntity> CreateFakedDoc()
        {
            return new Faker<DocumentEntity>()
           .StrictMode(true)
           .RuleFor(p => p.Reference, p => Guid.NewGuid().ToString())
           .RuleFor(p => p.Name, f => f.Commerce.Product())
           .RuleFor(p => p.Content, f => new BinaryData(DownloadImage(f.Image.LoremFlickrUrl()).GetAwaiter().GetResult()))
           .RuleFor(p => p.MimeType, f => "image/png")
           .RuleFor(p => p.Extension, f => "png")
           .RuleFor(p => p.DocumentType, f => f.PickRandom<DocumentType>())
           .RuleFor(p => p.Certificate, f => f.Random.Bytes(32))
           .RuleFor(p => p.Price, f => f.Random.Decimal(0, 320))
           .RuleFor(p => p.Created, f => DateTimeOffset.UtcNow)
           .RuleFor(p => p.Updated, f => f.Date.BetweenOffset(DateTime.UtcNow.AddYears(-4), DateTime.UtcNow))
           .RuleFor(p => p.Longitude, f => f.Random.Double())
           .RuleFor(p => p.Latitude, f => f.Random.Double())
           .RuleFor(p => p.Size, f => f.Random.Long(min: 1000, max: 100000));
        }

        public static Faker<HttpTraceEntity> CreateFakedHttpTraceEntity()
        {
            var fake = new Faker<HttpTraceEntity>()
            .StrictMode(true)
            .RuleFor(p => p.OperationId, p => Guid.NewGuid().ToString())
            .RuleFor(p => p.Name, f => f.PickRandom("Session_PUT", "Session_PATH", "Session_START", "Session_STOP"))
            .RuleFor(p => p.Body, f => new BinaryData(DownloadImage(f.Image.LoremFlickrUrl()).GetAwaiter().GetResult()))
            .RuleFor(p => p.BodyString, f =>  f.Lorem.Text())
            .RuleFor(p => p.MimeType, f => "image/png")
            .RuleFor(p => p.OperationType, f => f.PickRandom<OperationType>())
            .RuleFor(p => p.Host, f => f.Image.LoremFlickrUrl())
            .RuleFor(p => p.Timestamp, f => DateTime.UtcNow)
            .RuleFor(p => p.HttpMethod, f => f.PickRandom<HttpMethod>())
            .RuleFor(p => p.BodyObject,f=> null);

            

            return fake;
        }

        public static Faker<PersonEntity> CreateFakePerson(string[] accounts = null)
        {
            var rankid = 0;
            _ = accounts ?? Enumerable.Range(1, 5).Select(a => Guid.NewGuid().ToString()).ToArray();
            var fake = new Faker<PersonEntity>()
            //Ensure all properties have rules. By default, StrictMode is false
            //Set a global policy by using Faker.DefaultStrictMode
            .StrictMode(true)
            //OrderId is deterministic
            .RuleFor(p => p.TenantId, f => (accounts==null)?Guid.NewGuid().ToString(): f.PickRandom(accounts))
            .RuleFor(p => p.PersonId, f => Guid.NewGuid())
            .RuleFor(p => p.Rank, f => rankid++)
            .RuleFor(p => p.Address, f => FakedAddress())

            //A nullable int? with 80% probability of being null.
            //The .OrNull extension is in the Bogus.Extensions namespace.
            .RuleFor(p => p.OtherAddress, f => FakedAddress().Generate(5))
            .RuleFor(p => p.Created, f => f.Date.BetweenOffset(DateTime.UtcNow.AddYears(-4), DateTime.UtcNow))
            .RuleFor(p => p.Enabled, f => f.Random.Bool())
            .RuleFor(p => p.FirstName, f => f.Person.FirstName)
            .RuleFor(p => p.LastName, f => f.Person.LastName)
            .RuleFor(p => p.Longitude, f => f.Random.Double())
            .RuleFor(p => p.Latitude, f => f.Random.Double())
            .RuleFor(p => p.Distance, f => f.Random.Decimal())
            .RuleFor(p => p.Altitude, f => f.Random.Decimal().OrNull(f))
            .RuleFor(p => p.Genre, f => f.Random.Enum<Genre>())
            .RuleFor(p => p.Situation, f => f.Random.Enum<Situation>())
            .RuleFor(p => p.BankAmount, f => f.Random.Float())
            .RuleFor(p => p.ThumbPrint, f => f.Random.Bytes(10));

            return fake;
        }

        public static Faker<Address> FakedAddress()
        {
            var fake = new Faker<Address>()
            .RuleFor(a => a.ZipCode, f => f.Address.ZipCode())
            .RuleFor(a => a.Street, f => $"{f.Address.StreetName()} {f.Address.StreetSuffix()} {f.Address.StreetAddress()}")
            .RuleFor(a => a.State, f => f.Address.State())
            .RuleFor(a => a.Country, f => f.Address.Country())
            .RuleFor(a => a.City, f => f.Address.City())
            .RuleFor(a => a.AdressType, f => f.PickRandom<AdressType>());
            return fake;
        }
    }
}