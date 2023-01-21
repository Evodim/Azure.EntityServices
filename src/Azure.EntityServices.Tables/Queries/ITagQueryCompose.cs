using Azure.EntityServices.Queries;
using System;
using System.Linq.Expressions;

namespace Azure.EntityServices.Tables
{
    /// <summary>
    /// Extend and compose Query filter with dedicated Entity tag operations
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface ITagQueryCompose<T> : IQuery<T>, ITagQueryFilter<T>
    {
        ITagQueryFilter<T, P> AddQuery<P>(Expression<Func<T, P>> property);

        ITagQueryFilter<T> AddQuery(string property);
    }
}