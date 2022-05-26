using System;
using System.Collections.Generic;

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
    }
}