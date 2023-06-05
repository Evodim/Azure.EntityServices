using System.Collections.Generic;

namespace Azure.EntityServices.Tables
{
    public record struct EntityPage<T>(IEnumerable<T> Entities, int IteratedCount, bool IsLastPage, string ContinuationToken)
    {
        public static implicit operator (IEnumerable<T>, int iteratorCount, bool isLastPage, string ContinuationToken)(EntityPage<T> value)
        {
            return (value.Entities, value.IteratedCount, value.IsLastPage, value.ContinuationToken);
        }

        public static implicit operator EntityPage<T>((IEnumerable<T> Item1, int IteratorCount, string ContinuationToken) value)
        {
            return new EntityPage<T>(value.Item1, value.IteratorCount, string.IsNullOrEmpty(value.ContinuationToken), value.ContinuationToken);
        }
    }
}