using Azure.EntityServices.Queries.Core;
using System;

namespace Azure.EntityServices.Blobs
{
    public class BlobTagQueryInstructions : InstructionsProviderBase, IQueryInstructions
    {
        public string And => "AND";

        public string Not => throw new NotSupportedException("Blob tag 'Not' instruction not supported");

        public string Or => "OR";

        public string Equal => "=";

        public string NotEqual => "<>";

        public string GreaterThan => ">";

        public string GreaterThanOrEqual => ">=";

        public string LessThan => "<";

        public string LessThanOrEqual => "<=";
    }
}