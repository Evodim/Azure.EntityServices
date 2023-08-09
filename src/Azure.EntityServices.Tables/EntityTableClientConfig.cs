using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;

namespace Azure.EntityServices.Tables
{
    public class EntityTableClientConfig<T>
    {
        public Func<T, string> PartitionKeyResolver { get; set; }
        public Func<T, object> RowKeyResolver { get; set; }
        public IDictionary<string, Func<T, object>> ComputedProps { get; } = new Dictionary<string, Func<T, object>>();
        public IList<string> ComputedTags { get; } = new List<string>();
        public IDictionary<string, PropertyInfo> Tags { get; } = new Dictionary<string, PropertyInfo>();
        public ConcurrentDictionary<string, Func<IEntityObserver<T>>> Observers { get; } = new ConcurrentDictionary<string, Func<IEntityObserver<T>>>();
        public PropertyInfo RowKeyProp { get; set; }
        public IList<string> IgnoredProps { get; } = new List<string>();
    }
}