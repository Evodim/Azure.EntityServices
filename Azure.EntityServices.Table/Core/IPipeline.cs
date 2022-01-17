using System.Threading;
using System.Threading.Tasks;

namespace Azure.EntityServices.Table.Core
{
    internal interface IPipeline
    {
        Task CompleteAsync();

        Task SendAsync(EntityTransactionGroup entityTransactionGroup, CancellationToken cancellationToken = default);
    }
}