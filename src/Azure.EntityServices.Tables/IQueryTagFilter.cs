using Azure.EntityServices.Queries;
using System;

namespace Azure.EntityServices.Tables
{
    public interface IQueryTagFilter<T, P> : IQueryFilter<T, P>
    {
        public string TagName { get; }
        public Func<string, object, string> TagValueBuilder { get; }
    }

    public interface IQueryTagFilter<T> : IQueryTagFilter<T, object>, IQueryFilter<T>
    {
    }
}