using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;

namespace Azure.EntityServices.Tables
{
    public class EntityTableClientConfig<T>
    { 
        public Func<T, string> PartitionKeyResolver { get; set; }
        public IDictionary<string, Func<T, object>> DynamicProps { get; } = new Dictionary<string, Func<T, object>>();
        public IList<string> ComputedTags { get; } = new List<string>();
        public Dictionary<string, PropertyInfo> Tags { get; } = new Dictionary<string, PropertyInfo>();
        public ConcurrentDictionary<string, IEntityObserver<T>> Observers { get; } = new ConcurrentDictionary<string, IEntityObserver<T>>();
        public PropertyInfo PrimaryKeyProp { get; set; }
        public ICollection<string> IgnoredProps { get; } = new List<string>();
    }
}