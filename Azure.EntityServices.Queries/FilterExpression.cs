using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace Azure.EntityServices.Queries
{
    public class FilterExpression<T, P> : FilterExpression<T>, IQueryFilter<T, P>
    {
        public IFilterOperator<T> AddFilterCondition(string comparison, P value) => base.AddFilterCondition(comparison, value);
    }

    public class FilterExpression<T> : IFilterExpression<T>
    {
        public string PropertyName { get; set; }
        public Type PropertyType { get; set; }
        public string Comparator { get; set; }
        public string Operator { get; set; }
        public string GroupOperator { get; set; }
        public object PropertyValue { get; set; }
        public List<IFilterExpression<T>> Group { get; } = new List<IFilterExpression<T>>();

        public IFilterExpression<T> NextOperation { get; set; }

        public IQueryFilter<T, P> AddOperator<P>(string expressionOperator, Expression<Func<T, P>> property)
        {
            Operator = expressionOperator;
            var prop = property.GetPropertyInfo() ?? throw new InvalidFilterCriteriaException("Given Expression should be a valid property selector");
            var newOperation = new FilterExpression<T, P>()
            {
                PropertyName = prop.Name,
                PropertyType = prop.PropertyType
            };
            NextOperation = newOperation;
            return newOperation;
        }

        public IQueryFilter<T> AddOperator(string expressionOperator, string property)
        {
            Operator = expressionOperator;
            var newOperation = new FilterExpression<T>()
            {
                PropertyName = property,
                PropertyType = typeof(object)
            };
            NextOperation = newOperation;
            return newOperation;
        }

        public IFilterOperator<T> AddFilterCondition(string comparison, object value)
        {
            PropertyValue = value;
            Comparator = comparison;
            PropertyType = value?.GetType() ?? typeof(object);
            return this;
        }

        public IFilterOperator<T> AddGroupExpression(string expressionOperator, Action<IQueryCompose<T>> subQuery)
        {
            var childExpression = new FilterExpression<T>();
            subQuery.Invoke(childExpression);

            childExpression.GroupOperator = expressionOperator;
            Group.Add(childExpression);

            return this;
        }

        public IQueryFilter<T> AddQuery(string property)
        {
            return AddOperator(null, property);
        }

        public IQueryFilter<T, P> AddQuery<P>(Expression<Func<T, P>> property)
        {
            return AddOperator(null, property);
        }
    }
}