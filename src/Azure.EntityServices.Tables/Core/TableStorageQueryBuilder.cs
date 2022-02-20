using Azure.EntityServices.Queries;
using Azure.EntityServices.Queries.Core;
using Azure.EntityServices.Tables.Extensions;
using Microsoft.Azure.Cosmos.Table;
using System;

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
            if (expression.PropertyType == typeof(byte[]))
                return TableQuery.GenerateFilterConditionForBinary(expression.PropertyName, GetInstruction(expression.Comparator), (byte[])expression.PropertyValue);

            if (expression.PropertyType == typeof(bool) || expression.PropertyType == typeof(bool?))
                return TableQuery.GenerateFilterConditionForBool(expression.PropertyName, GetInstruction(expression.Comparator), (bool)expression.PropertyValue);

            if (expression.PropertyType == typeof(DateTime) || expression.PropertyType == typeof(DateTime?))
                return TableQuery.GenerateFilterConditionForDate(expression.PropertyName, GetInstruction(expression.Comparator), (DateTime)expression.PropertyValue);

            if (expression.PropertyType == typeof(DateTimeOffset) || expression.PropertyType == typeof(DateTimeOffset?))
                return TableQuery.GenerateFilterConditionForDate(expression.PropertyName, GetInstruction(expression.Comparator), (DateTimeOffset)expression.PropertyValue);

            if (expression.PropertyType == typeof(double) || expression.PropertyType == typeof(double?))
                return TableQuery.GenerateFilterConditionForDouble(expression.PropertyName, GetInstruction(expression.Comparator), (double)expression.PropertyValue);

            if (expression.PropertyType == typeof(Guid) || expression.PropertyType == typeof(Guid?))
                return TableQuery.GenerateFilterConditionForGuid(expression.PropertyName, GetInstruction(expression.Comparator), (Guid)expression.PropertyValue);

            if (expression.PropertyType == typeof(int) || expression.PropertyType == typeof(int?))
                return TableQuery.GenerateFilterConditionForInt(expression.PropertyName, GetInstruction(expression.Comparator), (int)expression.PropertyValue);

            if (expression.PropertyType == typeof(long) || expression.PropertyType == typeof(long?))
                return TableQuery.GenerateFilterConditionForLong(expression.PropertyName, GetInstruction(expression.Comparator), (long)expression.PropertyValue);

            return TableQuery.GenerateFilterCondition(expression.PropertyName, GetInstruction(expression.Comparator), expression.PropertyValue.ToInvariantString());
        }
    }
}