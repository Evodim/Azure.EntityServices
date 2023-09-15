using System;
using System.Threading.Tasks;

namespace Azure.EntityServices.Tables.Core.Abstractions
{
    public interface IAsyncObserver<in T>
    {
        Task OnNextAsync(T item);
        Task OnCompletedAsync();
        Task OnErrorAsync(Exception ex);
    }
}