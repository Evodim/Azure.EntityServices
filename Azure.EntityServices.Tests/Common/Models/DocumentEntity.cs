using System;

namespace Azure.EntityServices.Tests.Common.Models
{
    public class DocumentEntity
    {
        public string Reference { get; set; }
        public string Name { get; set; }
        public DateTimeOffset Created { get; set; }
        public DateTimeOffset? Updated { get; set; }
        public double? Latitude { get; set; }
        public double? Longitude { get; set; }
        public DocumentType DocumentType { get; set; }
        public string MimeType { get; set; } 
        public string Extension { get; set; }
        public long Size { get; set; }
        public decimal Price { get; set; }
        public byte[] Certificate { get; set; }
        public BinaryData Content { get; set; }
    }
}