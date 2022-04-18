using Azure.EntityServices.Queries;
using Azure.EntityServices.Tables.Core;

namespace Azure.EntityServices.Tables
{
    /// <summary>
    /// Extend table query capabilities to use it for tag filters
    /// </summary>
    public static class ITagQueryFilterExtensions
    {
        public static IFilterOperator<T> Between<T, P>(this ITagQueryFilter<T, P> query, P minValue,P maxValue)
        {
            return (query as IQueryFilter<T>)
                  .GreaterThan($"{TableQueryHelper.ToRowKey(query.TagName, minValue)}$")
                  .And($"RowKey")
                  .LessThan($"{TableQueryHelper.ToRowKey(query.TagName, maxValue)}$~")
                  .And(EntitytableConstants.DeletedTag).Equal(false);
        }
        public static IFilterOperator<T> Equal<T, P>(this ITagQueryFilter<T, P> query, P value)
        {
            return (query as IQueryFilter<T>)
                  .GreaterThan($"{TableQueryHelper.ToRowKey(query.TagName, value)}$")
                  .And($"RowKey")
                  .LessThan($"{TableQueryHelper.ToRowKey(query.TagName, value)}$~")
                  .And(EntitytableConstants.DeletedTag).Equal(false);
        }
     

        public static IFilterOperator<T> GreaterThan<T, P>(this ITagQueryFilter<T, P> query, P value)
        {
            return (query as IQueryFilter<T>)
               .GreaterThan($"{TableQueryHelper.ToRowKey(query.TagName, value)}$~")
               .And($"RowKey")
               .LessThan($"{query.TagName}-~")
               .And(EntitytableConstants.DeletedTag).Equal(false);
        }

        public static IFilterOperator<T> GreaterThanOrEqual<T, P>(this ITagQueryFilter<T> query, P value)
        {
            return (query as IQueryFilter<T>)
               .GreaterThan($"{TableQueryHelper.ToRowKey(query.TagName,value)}$")
               .And($"RowKey")
               .LessThan($"{query.TagName}-~")
               .And(EntitytableConstants.DeletedTag).Equal(false);
        }
        public static IFilterOperator<T> LessThan<T, P>(this ITagQueryFilter<T> query, P value)
        {
            return (query as IQueryFilter<T>)
               .GreaterThan($"{query.TagName}-")
               .And($"RowKey")
               .LessThan($"{TableQueryHelper.ToRowKey(query.TagName, value)}$")
               .And(EntitytableConstants.DeletedTag).Equal(false);
        }

        public static IFilterOperator<T> LessThanOrEqual<T, P>(this ITagQueryFilter<T> query, P value)
        {
            return (query as IQueryFilter<T>)
               .GreaterThan($"{query.TagName}-")
               .And($"RowKey")
               .LessThan($"{TableQueryHelper.ToRowKey(query.TagName, value)}$~")
               .And(EntitytableConstants.DeletedTag).Equal(false);
        }
     
    }
}