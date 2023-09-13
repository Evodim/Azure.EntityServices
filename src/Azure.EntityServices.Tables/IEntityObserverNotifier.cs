using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Azure.EntityServices.Tables
{
    public interface IEntityObserverNotifier<T> where T : class, new()
    {
        Task NotifyChangeAsync(IEnumerable<EntityOperationContext<T>> context);
        Task NotifyCompleteAsync();
        Task NotifyExceptionAsync(Exception ex);
    }
}