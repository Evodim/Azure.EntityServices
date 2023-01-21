using System.Collections.Generic;

namespace Azure.EntityServices.Tables
{
    public record struct EntityPage<T>(IEnumerable<T> Entities, string ContinuationToken)
    {
        public static implicit operator (IEnumerable<T>, string ContinuationToken)(EntityPage<T> value)
        {
            return (value.Entities, value.ContinuationToken);
        }

        public static implicit operator EntityPage<T>((IEnumerable<T>, string ContinuationToken) value)
        {
            return new EntityPage<T>(value.Item1, value.ContinuationToken);
        }
    }
}