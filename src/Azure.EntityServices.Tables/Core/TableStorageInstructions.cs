using Azure.EntityServices.Queries.Core;

namespace Azure.EntityServices.Tables.Core
{
    public class TableStorageInstructions : InstructionsProviderBase, IQueryInstructions
    {
        public string And => "and";

        public string AndNot => "and not";

        public string Or => "or";

        public string Equal => "eq";

        public string NotEqual => "ne";

        public string GreaterThan => "gt";

        public string GreaterThanOrEqual => "ge";

        public string LessThan => "lt";

        public string LessThanOrEqual => "le";

        public string OrNot => "or not";
    }
}