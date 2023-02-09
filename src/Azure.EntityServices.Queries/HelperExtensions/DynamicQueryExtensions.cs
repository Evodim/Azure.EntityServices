using System;
using System.Collections.Generic;
using System.Linq;
using System.Xml.XPath;

namespace Azure.EntityServices.Queries
{
    public static class DynamicQueryExtensions
    {
        /// <summary>
        /// Build query filters by iterate on each given collection item
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="U">Generic collection</typeparam>
        /// <param name="query"></param>
        /// <param name="list"></param>
        /// <param name="action"></param>
        /// <returns></returns>
        public static IFilterOperator<T> WithEach<T, U>(this IFilterOperator<T> query, IEnumerable<U> list, Func<U, IFilterOperator<T>, IFilterOperator<T>> action)
        {
            var dynamicQuery = query;
            foreach (var item in list)
            {
                dynamicQuery = action(item, dynamicQuery);
            }
            return dynamicQuery;
        } 
        /// <summary>
        /// Build a filter to check if current field value was not present in given list
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="P"></typeparam>
        /// <param name="query"></param>
        /// <param name="values"></param>
        /// <returns></returns>
        public static IFilterOperator<T> NotIn<T, P>(this IQueryFilter<T, P> query, params P[] values)
        {
            var exp = query as IFilterExpression<T>;

            exp.LastOperation.AddGroupExpression(string.IsNullOrEmpty(exp.LastOperation.PropertyName) ? "" : "And",
                p => p.Where(exp.PropertyName)
                ._NotIn(values));
            exp.PropertyName = null;
            exp.Operator = null;

            return exp.LastOperation;
        }
        /// <summary>
        /// Build a filter to check if current field value was present in given list
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="P"></typeparam>
        /// <param name="query"></param>
        /// <param name="values"></param>
        /// <returns></returns>
        public static IFilterOperator<T> In<T, P>(this IQueryFilter<T, P> query, params P[] values)
        {

            var exp = query as IFilterExpression<T>;

            exp.LastOperation.AddGroupExpression(string.IsNullOrEmpty(exp.LastOperation.PropertyName) ? "" : "And",
                p => p.Where(exp.PropertyName)
                ._In(values));
            exp.PropertyName = null;
            exp.Operator = null;

            return exp.LastOperation;
        }
     
        private static IFilterOperator<T> _In<T, P>(this IQueryFilter<T> query, P[] values)
        {
            IQuery<T> nextQuery = (IQuery<T>)query;
            foreach (var item in values.SkipLast(1))
            {
                nextQuery = (IQuery<T>)(nextQuery as IQueryFilter<T>)
                    .Equal(item)
                    .Or((query as IFilterExpression<T>).PropertyName);
            }
            (nextQuery as IQueryFilter<T>).Equal(values.Last());
            return nextQuery as IFilterOperator<T>;
        }
     
        private static IFilterOperator<T> _NotIn<T, P>(this IQueryFilter<T> query, P[] values)
        {
            IQuery<T> nextQuery = (IQuery<T>)query;
            foreach (var item in values.SkipLast(1))
            {
                nextQuery = (IQuery<T>)(nextQuery as IQueryFilter<T>)
                    .NotEqual(item)
                    .And((query as IFilterExpression<T>).PropertyName);
            }
            (nextQuery as IQueryFilter<T>).NotEqual(values.Last());
            return nextQuery as IFilterOperator<T>;
        }

    }
}