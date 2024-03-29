﻿namespace Azure.EntityServices.Queries.Core
{
    public interface IQueryInstructions : IOperatorInstructions, IComparatorInstructions
    {
    }

    public interface IOperatorInstructions
    {
        string And { get; }        
        string Or { get; }
        string AndNot { get; }
        string OrNot { get; }
    }

    public interface IComparatorInstructions
    {
        string Equal { get; }
        string NotEqual { get; }
        string GreaterThan { get; }
        string GreaterThanOrEqual { get; }
        string LessThan { get; }
        string LessThanOrEqual { get; }
    }
}