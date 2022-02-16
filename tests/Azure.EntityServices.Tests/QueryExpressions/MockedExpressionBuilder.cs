using Azure.EntityServices.Queries;
using Azure.EntityServices.Queries.Core;

namespace Azure.EntityServices.Table.Tests
{
    public class MockedExpressionBuilder<T> : BaseQueryExpressionBuilder<T>
    {
        public MockedExpressionBuilder() : base(new FilterExpression<T>(), new MockedInstructionsProvider())
        {
        }
    }
}