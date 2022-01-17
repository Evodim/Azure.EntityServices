using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;

namespace Azure.EntityServices.Table
{
    public class EntityTableClientConfig<T>
    {
        public Func<T, string> PartitionKeyResolver { get; set; }
        public Dictionary<string, Func<T, object>> DynamicProps { get; } = new Dictionary<string, Func<T, object>>();
        public List<string> ComputedTags { get; } = new List<string>();
        public Dictionary<string, PropertyInfo> Tags { get; } = new Dictionary<string, PropertyInfo>();
        public ConcurrentDictionary<string, IEntityObserver<T>> Observers { get; } = new ConcurrentDictionary<string, IEntityObserver<T>>();
        public PropertyInfo PrimaryProp { get; set; }
    }
}