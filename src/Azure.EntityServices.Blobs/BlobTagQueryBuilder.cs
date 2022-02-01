using Azure.EntityServices.Blobs.Extensions;
using Azure.EntityServices.Queries;
using Azure.EntityServices.Queries.Core;

namespace Azure.EntityServices.Blobs

{
    //Azure blob tag query  implementation of QueryExpressionBuilder
    public class BlobTagQueryBuilder<T> : BaseQueryExpressionBuilder<T>
    {
        public BlobTagQueryBuilder() : base(new FilterExpression<T>(), new BlobTagQueryInstructions())
        {
        }

        public BlobTagQueryBuilder(IFilterExpression<T> expression) : base(expression, new BlobTagQueryInstructions())
        {
        }

        private string GetInstruction(string instructionName) => InstructionsProvider.Get(instructionName);

        protected override string ExpressionFilterConverter(IFilterExpression<T> expression)
        {
            if (expression.PropertyName.StartsWith("@"))
            {
                return $"{expression.PropertyName} {GetInstruction(expression.Comparator)} '{expression.PropertyValue.ToInvariantString()}'";
            }
            else
            {
                return $"\"{expression.PropertyName}\" {GetInstruction(expression.Comparator)} '{expression.PropertyValue.ToInvariantString()}'";
            }
        }
    }
}