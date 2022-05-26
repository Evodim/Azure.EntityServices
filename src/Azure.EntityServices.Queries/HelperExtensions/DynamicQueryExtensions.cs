using System;
using System.Collections.Generic;

namespace Azure.EntityServices.Queries
{
    public static class DynamicQueryExtensions
    {
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