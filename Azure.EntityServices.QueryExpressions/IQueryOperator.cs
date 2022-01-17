using System;
using System.Linq.Expressions;

namespace Azure.EntityServices.Queries
{
    public interface IFilterOperator<T>
    {
        IQueryFilter<T, P> AddOperator<P>(string expressionOperator, Expression<Func<T, P>> property);

        IQueryFilter<T> AddOperator(string expressionOperator, string property);

        IFilterOperator<T> AddGroupExpression(string expressionOperator, Action<IQueryCompose<T>> subQuery);
    }
}