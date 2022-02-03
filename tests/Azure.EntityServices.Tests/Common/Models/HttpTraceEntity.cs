using System;

namespace Azure.EntityServices.Tests.Common.Models
{
     
    public class HttpTraceEntity
    {
        public OperationType OperationType { get; set; }
        public HttpMethod HttpMethod { get; set; }
        public string Host { get; set; }
        public string OperationId { get; set; }
        public DateTime Timestamp { get; set; }
        public string MimeType { get; set; }
        public string Name { get; set; }
        public BinaryData Body { get; set; }
        public string BodyString { get; set; }
        public object BodyObject { get; set; }
       
    }
}