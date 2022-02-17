using System;
using System.Linq.Expressions;

namespace Azure.EntityServices.Tables
{
    public interface ITagQueryCompose<T> : IQueryTagFilter<T>
    {
        IQueryTagFilter<T, P> AddTagQuery<P>(Expression<Func<T, P>> property);

        IQueryTagFilter<T> AddTagQuery(string property);
    }
}