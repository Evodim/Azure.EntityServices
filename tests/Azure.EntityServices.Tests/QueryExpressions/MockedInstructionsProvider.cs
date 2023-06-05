using Azure.EntityServices.Queries.Core;

namespace Azure.EntityServices.Table.Tests
{
    public class MockedInstructionsProvider : InstructionsProviderBase, IQueryInstructions
    {
        public string And => "And";

        public string Not => "Not";

        public string Or => "Or";

        public string Equal => "Equal";

        public string NotEqual => "NotEqual";

        public string GreaterThan => "GreaterThan";

        public string GreaterThanOrEqual => "GreaterThanOrEqual";

        public string LessThan => "LessThan";

        public string LessThanOrEqual => "LessThanOrEqual";

        public string AndNot => "And Not";

        public string OrNot => "Or Not";
    }
}