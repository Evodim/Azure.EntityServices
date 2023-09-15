using Azure.Data.Tables;
using Azure.EntityServices.Tables.Core.Abstractions;
using System.Collections.Generic;

namespace Azure.EntityServices.Tables.Core
{
    public class TableEntityDataReader<T> : IEntityDataReader<T>
        where T : class, new()
    {
        private readonly IEntityAdapter<T> _entityAdapter;
        private readonly IDictionary<string, object> _nativeProperties;

        public TableEntityDataReader(IDictionary<string,object> nativeProperties, IEntityAdapter<T> entityAdapter)
        {
            _entityAdapter = entityAdapter;
            _nativeProperties = nativeProperties;
        }

        public T Read()
        {
            return _entityAdapter.FromEntityModel(_nativeProperties);
        }

        public IDictionary<string, object> ReadMetadata()
        {
            return _entityAdapter.GetMetadata(_nativeProperties);
        }
    }
}