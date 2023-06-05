using System;
using System.Collections.Generic;

namespace Common.Samples.Models
{
    public enum OperationType
    {
        Request,
        Response,
    }

    public enum HttpMethod
    {
        GET,
        PUT,
        PATCH,
        DELETE
    }

    public enum Genre
    {
        Male,
        Female
    }

    public enum Situation
    {
        Single,
        Married,
        Divorced
    }

    public struct GeoPosition
    {
        public double Latitude { get; set; }
        public double Longitude { get; set; }
    }

    public enum DocumentType
    {
        Contract,
        Avenant,
        Termination
    }

    public class PersonEntity
    {
        public string TenantId { get; set; }
        public DateTimeOffset? Created { get; set; }
        public DateTime? LocalCreated { get; set; } 
        public DateTimeOffset Updated { get; set; } 
        public DateTime LocalUpdated { get; set; }
        public bool? Enabled { get; set; }
        public Address Address { get; set; }
        public List<Address> OtherAddresses { get; set; }
        public Guid PersonId { get; set; }
        public string FirstName { get; set; }
        public int? Rank { get; set; }
        public string LastName { get; set; }
        public double Latitude { get; set; }
        public double Longitude { get; set; }
        public decimal Distance { get; set; }
        public decimal? Altitude { get; set; }
        public float? BankAmount { get; set; }
        public string Type => nameof(PersonEntity);
        public Genre Genre { get; set; }
        public Situation? Situation { get; set; }
        public byte[] ThumbPrint { get; set; }
    }
}