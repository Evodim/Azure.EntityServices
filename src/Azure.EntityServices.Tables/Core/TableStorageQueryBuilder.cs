using Azure.EntityServices.Queries;
using Azure.EntityServices.Queries.Core;

namespace Azure.EntityServices.Tables.Core
{
    //Azure table storage implementation of QueryExpressionBuilder
    public class TableStorageQueryBuilder<T> : BaseQueryExpressionBuilder<T>
    {
        public TableStorageQueryBuilder(IFilterExpression<T> expression) : base(expression, new TableStorageInstructions())
        {
        }

        private string GetInstruction(string instructionName) => InstructionsProvider.Get(instructionName);

        protected override string ExpressionFilterConverter(IFilterExpression<T> expression)
        {
            return $"{expression.PropertyName} {GetInstruction(expression.Comparator)} {TableQueryHelper.ValueToString(expression.PropertyValue)}";
        }
    }
}