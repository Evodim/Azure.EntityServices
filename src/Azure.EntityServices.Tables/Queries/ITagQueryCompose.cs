using System;
using System.Linq.Expressions;

namespace Azure.EntityServices.Tables
{
    public interface ITagQueryCompose<T> : ITagQuery<T>, ITagQueryFilter<T>
    {
        ITagQueryFilter<T, P> AddTagQuery<P>(Expression<Func<T, P>> property);

        ITagQueryFilter<T> AddTagQuery(string property);
    }
}