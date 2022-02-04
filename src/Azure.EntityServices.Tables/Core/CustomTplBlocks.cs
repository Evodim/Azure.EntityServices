using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Azure.EntityServices.Tables.Core
{
    internal static class CustomTplBlocks
    {
        /// <summary>
        /// Create transaction entity pipeline
        /// </summary>
        /// <param name="asyncProcessor"></param>
        /// <param name="maxItemInTransaction"></param>
        /// <param name="maxParallelTasks"></param>
        /// <returns></returns>
        public static IPipeline CreatePipeline(Func<EntityTransactionGroup[], Task> asyncProcessor, int maxItemInBatch, int maxItemInTransaction, int maxParallelTasks)
        {
            //Create en configure transaction entity group flow pipepline
            var pipeline = new BatchBlock<EntityTransactionGroup>(maxItemInBatch, new GroupingDataflowBlockOptions()
            {
                Greedy = true,
                BoundedCapacity = maxItemInBatch
            });
            //define blocks
            var groupPerPartitionsBlock = new TransformBlock<EntityTransactionGroup[], IEnumerable<EntityTransactionGroup[]>>(v =>
            {
                return v.GroupBy(k => k.PartitionKey).Select(s => s.ToArray());
            }, new ExecutionDataflowBlockOptions()
            {
                MaxDegreeOfParallelism = 1,
                BoundedCapacity = 1
            });

            var transactionGroupBlock = CreatePartitionedBlock(maxItemInTransaction, maxParallelTasks);

            var target = new ActionBlock<EntityTransactionGroup[]>(asyncProcessor,
                new ExecutionDataflowBlockOptions()
                {
                    BoundedCapacity = maxParallelTasks,
                    MaxDegreeOfParallelism = maxParallelTasks
                });

            //link blocks together
            pipeline.LinkTo(groupPerPartitionsBlock, new DataflowLinkOptions() { PropagateCompletion = true });
            groupPerPartitionsBlock.LinkTo(transactionGroupBlock, new DataflowLinkOptions() { PropagateCompletion = true });
            transactionGroupBlock.LinkTo(target, new DataflowLinkOptions() { PropagateCompletion = true });

            return new Pipeline(pipeline, target);
        }

        /// <summary>
        /// Group entity transactions to be processed in a single operation according to azure table storage limitation:  max of 100 entities table for a same partition
        /// </summary>
        private static IPropagatorBlock<IEnumerable<EntityTransactionGroup[]>, EntityTransactionGroup[]> CreatePartitionedBlock(int maxItemInGroup, int maxParallelTasks)
        {
            var source = new BufferBlock<EntityTransactionGroup[]>(new DataflowBlockOptions()
            {
                BoundedCapacity = maxParallelTasks
            });

            var target = new ActionBlock<IEnumerable<EntityTransactionGroup[]>>(async partitions =>
            {
                foreach (var partition in partitions)
                {
                    var count = 0;
                    var group = new List<EntityTransactionGroup>();
                    foreach (var item in partition)
                    {
                        if (count + item.Actions.Count >= maxItemInGroup)
                        {
                            Interlocked.Exchange(ref count, 0);
                            await source.SendAsync(group.ToArray());
                            group.Clear();
                        }
                        group.Add(item);
                        Interlocked.Exchange(ref count, count + item.Actions.Count);
                    }
                    await source.SendAsync(group.ToArray());
                    group.Clear();
                }
            },new ExecutionDataflowBlockOptions{
                MaxDegreeOfParallelism = 1,
                BoundedCapacity = 1
            });

            target.Completion.ContinueWith(delegate
            {
                source.Complete();
            });

            return DataflowBlock.Encapsulate(target, source);
        }
    }
}