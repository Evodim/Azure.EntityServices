using Azure.EntityServices.Queries;

namespace Azure.EntityServices.Tables.Extensions
{
    /// <summary>
    /// Extend table query capabilities to use it for tag filters
    /// </summary>
    public static class IQueryTagFilterExtensions
    {
        public static IFilterOperator<T> Between<T, P>(this IQueryTagFilter<T, P> query, P minValue,P maxValue)
        {
            return (query as IQueryFilter<T>)
                  .GreaterThan($"{query.TagValueBuilder(query.TagName, minValue)}$")
                  .And($"RowKey")
                  .LessThan($"{query.TagValueBuilder(query.TagName, maxValue)}$~")
                  .And("_deleted_tag_").Equal(false);
        }
        public static IFilterOperator<T> Equal<T, P>(this IQueryTagFilter<T, P> query, P value)
        {
            return (query as IQueryFilter<T>)
                  .GreaterThan($"{query.TagValueBuilder(query.TagName, value)}$")
                  .And($"RowKey")
                  .LessThan($"{query.TagValueBuilder(query.TagName, value)}$~")
                  .And("_deleted_tag_").Equal(false);
        }
     

        public static IFilterOperator<T> GreaterThan<T, P>(this IQueryTagFilter<T, P> query, P value)
        {
            return (query as IQueryFilter<T>)
               .GreaterThan($"{query.TagValueBuilder(query.TagName, value)}$~")
               .And($"RowKey")
               .LessThan($"{query.TagName}-~")
               .And("_deleted_tag_").Equal(false);
        }

        public static IFilterOperator<T> GreaterThanOrEqual<T, P>(this IQueryTagFilter<T> query, P value)
        {
            return (query as IQueryFilter<T>)
               .GreaterThan($"{query.TagValueBuilder(query.TagName, value)}$")
               .And($"RowKey")
               .LessThan($"{query.TagName}-~")
               .And("_deleted_tag_").Equal(false);
        }
        public static IFilterOperator<T> LessThan<T, P>(this IQueryTagFilter<T> query, P value)
        {
            return (query as IQueryFilter<T>)
               .GreaterThan($"{query.TagName}-")
               .And($"RowKey")
               .LessThan($"{query.TagValueBuilder(query.TagName, value)}$")
               .And("_deleted_tag_").Equal(false);
        }

        public static IFilterOperator<T> LessThanOrEqual<T, P>(this IQueryTagFilter<T> query, P value)
        {
            return (query as IQueryFilter<T>)
               .GreaterThan($"{query.TagName}-")
               .And($"RowKey")
               .LessThan($"{query.TagValueBuilder(query.TagName, value)}$~")
               .And("_deleted_tag_").Equal(false);
        }
     
    }
}