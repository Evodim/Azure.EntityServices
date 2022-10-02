using Azure.EntityServices.Queries.Core;
using System;
using System.Linq.Expressions;

namespace Azure.EntityServices.Queries
{
    public static class QueryExpressionExtensions
    {
        public static IQueryFilter<T, P> Where<T, P>(this IQuery<T> query, Expression<Func<T, P>> property)
            => (query as IQueryCompose<T>).AddQuery(property);

        public static IQueryFilter<T> Where<T>(this IQuery<T> query, string property)
            => (query as IQueryCompose<T>).AddQuery(property);

        public static IFilterOperator<T> Equal<T, P>(this IQueryFilter<T, P> query, P value)
            => query.AddFilterCondition(nameof(IQueryInstructions.Equal), value);

        public static IFilterOperator<T> NotEqual<T, P>(this IQueryFilter<T, P> query, P value)
            => query.AddFilterCondition(nameof(IQueryInstructions.NotEqual), value);

        public static IFilterOperator<T> GreaterThan<T, P>(this IQueryFilter<T, P> query, P value)
            => query.AddFilterCondition(nameof(IQueryInstructions.GreaterThan), value);

        public static IFilterOperator<T> GreaterThanOrEqual<T, P>(this IQueryFilter<T, P> query, P value)
            => query.AddFilterCondition(nameof(IQueryInstructions.GreaterThanOrEqual), value);

        public static IFilterOperator<T> LessThan<T, P>(this IQueryFilter<T, P> query, P value)
            => query.AddFilterCondition(nameof(IQueryInstructions.LessThan), value);

        public static IFilterOperator<T> LessThanOrEqual<T, P>(this IQueryFilter<T, P> query, P value)
            => query.AddFilterCondition(nameof(IQueryInstructions.LessThanOrEqual), value);

        public static IQueryFilter<T, P> And<T, P>(this IFilterOperator<T> query, Expression<Func<T, P>> property)
            => query.AddOperator(nameof(IQueryInstructions.And), property);

        public static IQueryFilter<T> And<T>(this IFilterOperator<T> query, string property)
            => query.AddOperator(nameof(IQueryInstructions.And), property);

        public static IFilterOperator<T> And<T>(this IFilterOperator<T> query, Action<IQueryCompose<T>> subQuery)
            => (subQuery==null)? query : query.AddGroupExpression(nameof(IQueryInstructions.And), subQuery); 

        public static IFilterOperator<T> Or<T>(this IFilterOperator<T> query, Action<IQueryCompose<T>> subQuery)
            => (subQuery == null) ? query : query.AddGroupExpression(nameof(IQueryInstructions.Or), subQuery);

        public static IQueryFilter<T, P> Or<T, P>(this IFilterOperator<T> query, Expression<Func<T, P>> property)
            => query.AddOperator(nameof(IQueryInstructions.Or), property);

        public static IQueryFilter<T> Or<T>(this IFilterOperator<T> query, string property)
            => query.AddOperator(nameof(IQueryInstructions.Or), property);
         
        public static IFilterOperator<T> OrNot<T>(this IFilterOperator<T> query, Action<IQueryCompose<T>> subQuery)
      => (subQuery == null) ? query : query.AddGroupExpression(nameof(IQueryInstructions.OrNot), subQuery);
 
        public static IFilterOperator<T> AndNot<T>(this IFilterOperator<T> query, Action<IQueryCompose<T>> subQuery)
          => (subQuery == null) ? query : query.AddGroupExpression(nameof(IQueryInstructions.AndNot), subQuery);
    }
}