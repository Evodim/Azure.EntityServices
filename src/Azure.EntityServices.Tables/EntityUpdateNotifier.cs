using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Azure.EntityServices.Tables
{
    public class EntityUpdateNotifier<T>
        where T: class, new()
    {
        private readonly IEnumerable<IEntityObserver<T>> _entityObservers;

        public EntityUpdateNotifier(IEnumerable<IEntityObserver<T>> entityObservers)
        {
            _entityObservers = entityObservers;
        }
        public async Task NotifyChangeAsync(IEnumerable<EntityOperationContext<T>> context)
        {
            foreach (var observer in _entityObservers)
            {
                await observer.OnNextAsync(context);
            }
        }

        public async Task NotifyExceptionAsync(Exception ex)
        {
            foreach (var observer in _entityObservers)
            {
                await observer.OnErrorAsync(ex);
            }
        }

        public async Task NotifyCompleteAsync()
        {
            foreach (var observer in _entityObservers)
            {
                await observer.OnCompletedAsync();
            }
        }
    }
}