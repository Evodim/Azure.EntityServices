using System.Collections.Generic;

namespace Azure.EntityServices.Tables
{
    public record struct EntityPage<T>(IEnumerable<T> Entities,int skipped, bool isLastPage, string ContinuationToken)
    {
        public static implicit operator (IEnumerable<T>,int skipped, bool isLastPage, string ContinuationToken)(EntityPage<T> value)
        {
            return (value.Entities, value.skipped, value.isLastPage, value.ContinuationToken);
        }

        public static implicit operator EntityPage<T>((IEnumerable<T> Item1,int skipped, string ContinuationToken) value)
        {
            return new EntityPage<T>(value.Item1, value.skipped , string.IsNullOrEmpty(value.ContinuationToken), value.ContinuationToken);
        }
    }
}