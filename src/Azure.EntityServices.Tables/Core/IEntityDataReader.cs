using Azure.Data.Tables;
using System.Collections.Generic;

namespace Azure.EntityServices.Tables.Core
{
    public class TableEntityDataReader<T> : IEntityDataReader<T>
        where T : class, new()
    {
        private readonly IEntityAdapter<T, TableEntity> _entityAdapter;
        private readonly TableEntity _tableEntity;

        public TableEntityDataReader(TableEntity tableEntity, IEntityAdapter<T,TableEntity> entityAdapter ) {

            _entityAdapter = entityAdapter;
            _tableEntity = tableEntity;
        }
        public T Read()
        {
            return _entityAdapter.FromEntityModel(_tableEntity);
        }

         

        public IDictionary<string, object> ReadProperties()
        {
            return _entityAdapter.GetProperties(_tableEntity);
        }
    }
    public interface IEntityDataReader<out T> 
    { 
        IDictionary<string, object> ReadProperties();
        T Read();
    }
}