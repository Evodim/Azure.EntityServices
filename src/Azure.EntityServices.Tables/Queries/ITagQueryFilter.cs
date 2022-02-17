using Azure.EntityServices.Queries;
using System;

namespace Azure.EntityServices.Tables
{
    public interface ITagQueryFilter<T, P> : IQueryFilter<T, P>
    {
        public string TagName { get; }
        public Func<string, object, string> TagValueBuilder { get; }
    }

    public interface ITagQueryFilter<T> : ITagQueryFilter<T, object>, IQueryFilter<T>
    {
    }
}