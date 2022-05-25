﻿using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Azure.EntityServices.Tables.Core
{
    public class Pipeline : IPipeline
    {
        private readonly IPropagatorBlock<EntityTransactionGroup, EntityTransactionGroup[]> _pipeline;
        private readonly ITargetBlock<EntityTransactionGroup[]> _target;

        public Pipeline(IPropagatorBlock<EntityTransactionGroup, EntityTransactionGroup[]> pipeline, ITargetBlock<EntityTransactionGroup[]> target)
        {
            _pipeline = pipeline;
            _target = target;
        }

        public async Task SendAsync(EntityTransactionGroup entityTransactionGroup, CancellationToken cancellationToken = default)
        {
            await _pipeline.SendAsync(entityTransactionGroup, cancellationToken);
        }

        public Task CompleteAsync()
        {
            _pipeline.Complete();
            return Task.WhenAll(_pipeline.Completion, _target.Completion);
        }
    }
}