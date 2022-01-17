using System;
using System.Linq.Expressions;

namespace Azure.EntityServices.Queries
{
    public interface IQueryCompose<T>
    {
        IQueryFilter<T, P> AddQuery<P>(Expression<Func<T, P>> property);

        IQueryFilter<T> AddQuery(string property);
    }
}