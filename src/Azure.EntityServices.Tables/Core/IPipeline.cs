using System.Threading;
using System.Threading.Tasks;

namespace Azure.EntityServices.Tables.Core
{
    internal interface IEntityTransactionGroupPipeline
    {
        Task CompleteAsync();

        Task SendAsync(EntityTransactionGroup entityTransactionGroup, CancellationToken cancellationToken = default);
    }
}