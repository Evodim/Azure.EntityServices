using System;

namespace Azure.EntityServices.Queries
{
    public interface IQueryFilter<T> : IQueryFilter<T, object>
    { }

    public interface IQueryFilter<T, P>
    {
        string PropertyName { get; }

        IFilterOperator<T> AddFilterCondition(string comparison, P value);

        IFilterOperator<T> AddFilterCondition(string comparison, object value, Type type);

    }
}