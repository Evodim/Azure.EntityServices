﻿using Azure.EntityServices.Queries;
using System;
using System.Linq.Expressions;

namespace Azure.EntityServices.Tables
{
    public class TagFilterExpression<T, P> : TagFilterExpression<T>, ITagQueryFilter<T, P>
    {
        public TagFilterExpression(string tagName, Func<string, object, string> tagValueBuilder) : base(tagName, tagValueBuilder)
        {
        }

        public IFilterOperator<T> AddFilterCondition(string comparison, P value)
        {
            return base.AddFilterCondition(comparison, value);
        }
    }

    public class TagFilterExpression<T> : FilterExpression<T>, ITagQueryCompose<T>
    {
        public string TagName { get; }

        public Func<string, object, string> TagValueBuilder { get; }

        public TagFilterExpression(string tagName, Func<string, object, string> tagValueBuilder)
        {
            TagName = tagName;
            TagValueBuilder = tagValueBuilder;
        }

        public ITagQueryFilter<T, P> AddTagQuery<P>(Expression<Func<T, P>> property)
        {
            return AddQuery(property) as ITagQueryFilter<T, P>;
        }

        public ITagQueryFilter<T> AddTagQuery(string property)
        {
            return AddQuery(property) as ITagQueryFilter<T>;
        }

        public override IFilterExpression<T> Factory()
        {
            return new TagFilterExpression<T>(TagName, TagValueBuilder);
        }

        public override IFilterExpression<T> Factory<P>()
        {
            return new TagFilterExpression<T, P>(TagName, TagValueBuilder);
        }
    }
}