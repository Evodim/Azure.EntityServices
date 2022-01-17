using System;
using System.Runtime.Serialization;

namespace Azure.EntityServices.Table

{
    [Serializable]
    public class EntityTableClientException : Exception
    {
        public EntityTableClientException()
        {
        }

        public EntityTableClientException(Exception _)
        {
        }

        public EntityTableClientException(string message) : base(message)
        {
        }

        public EntityTableClientException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected EntityTableClientException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}