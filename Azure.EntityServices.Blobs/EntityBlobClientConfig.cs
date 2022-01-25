using System;
using System.Collections.Generic;
using System.Reflection;

namespace Azure.EntityServices.Blobs
{
    public class EntityBlobClientConfig<T>
    {
        public IDictionary<string, Func<T, object>> ComputedProps { get; } = new Dictionary<string, Func<T, object>>();
        public IDictionary<string, PropertyInfo> Indexes { get; } = new Dictionary<string, PropertyInfo>();
        public IDictionary<string, PropertyInfo> IgnoredProps { get; } = new Dictionary<string, PropertyInfo>();
        public List<string> ComputedIndexes { get; } = new List<string>();
        public PropertyInfo ContentProp { get; set; }
        public PropertyInfo ReferenceProp { get; set; }

    }
}