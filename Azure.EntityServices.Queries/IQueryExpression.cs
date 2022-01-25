using System;
using System.Collections.Generic;

namespace Azure.EntityServices.Queries
{
    public interface IFilterExpression<T> : IFilterOperator<T>, IQueryFilter<T>, IQueryCompose<T>
    {
        string PropertyName { get; set; }
        Type PropertyType { get; set; }
        object PropertyValue { get; set; }
        string Comparator { get; set; }
        string Operator { get; set; }
        List<IFilterExpression<T>> Group { get; }
        public string GroupOperator { get; set; }
        IFilterExpression<T> NextOperation { get; set; }
    }
}