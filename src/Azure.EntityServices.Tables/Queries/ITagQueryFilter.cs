using Azure.EntityServices.Queries;

namespace Azure.EntityServices.Tables
{

    /// <summary>
    /// Handle and persist tag in a query filter 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <typeparam name="P"></typeparam>
    public interface ITagQueryFilter<T, P> : IQueryFilter<T, P>
    {
        public string TagName { get; set; }
    }

    public interface ITagQueryFilter<T> : ITagQueryFilter<T, object>
    {
    }
}