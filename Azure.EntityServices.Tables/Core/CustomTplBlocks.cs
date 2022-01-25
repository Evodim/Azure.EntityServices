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
        /// <param name="maxItemPerTransaction"></param>
        /// <param name="maxParallelTasks"></param>
        /// <returns></returns>
        public static IPipeline CreatePipeline(Func<EntityTransactionGroup[], Task> asyncProcessor, int maxItemPerTransaction, int maxParallelTasks)
        {
            //Create en configure transaction entity group flow pipepline
            var pipeline = new BatchBlock<EntityTransactionGroup>(1000, new GroupingDataflowBlockOptions() { Greedy = true, BoundedCapacity = 1000 });
            //define blocks
            var groupPerPartitions = new TransformBlock<EntityTransactionGroup[], IEnumerable<EntityTransactionGroup[]>>(v =>
            {
                return v.GroupBy(k => k.PartitionKey).Select(s => s.ToArray());
            }, new ExecutionDataflowBlockOptions()
            {
                BoundedCapacity = 1
            });

            var entityTransactionBatchGroup = CreateAtomicEntityTransactionGroupBlock(maxItemPerTransaction);

            var target = new ActionBlock<EntityTransactionGroup[]>(asyncProcessor, new ExecutionDataflowBlockOptions() { BoundedCapacity = maxParallelTasks, MaxDegreeOfParallelism = maxParallelTasks });

            //link blocks together
            pipeline.LinkTo(groupPerPartitions, new DataflowLinkOptions() { PropagateCompletion = true });
            groupPerPartitions.LinkTo(entityTransactionBatchGroup, new DataflowLinkOptions() { PropagateCompletion = true });
            entityTransactionBatchGroup.LinkTo(target, new DataflowLinkOptions() { PropagateCompletion = true });

            return new Pipeline(pipeline, target);
        }

        /// <summary>
        /// Group entity transactions to be processed in a single operation according to azure table storage limitation:  max of 100 entities table for a same partition
        /// </summary>
        private static IPropagatorBlock<IEnumerable<EntityTransactionGroup[]>, EntityTransactionGroup[]> CreateAtomicEntityTransactionGroupBlock(int batchSize)
        {
            var source = new BufferBlock<EntityTransactionGroup[]>(new DataflowBlockOptions() { BoundedCapacity = 1 });

            var target = new ActionBlock<IEnumerable<EntityTransactionGroup[]>>(async partitions =>
            {
                foreach (var partition in partitions)
                {
                    var count = 0;
                    var queue = new Queue<EntityTransactionGroup>();
                    foreach (var item in partition)
                    {
                        if (count + item.Actions.Count >= batchSize)
                        {
                            var data = queue.ToArray();
                            queue.Clear();
                            Interlocked.Exchange(ref count, 0);
                            await source.SendAsync(data);
                        }
                        queue.Enqueue(item);
                        Interlocked.Exchange(ref count, count + item.Actions.Count);
                    }
                    await source.SendAsync(queue.ToArray());
                    queue.Clear();
                }
            }, new ExecutionDataflowBlockOptions() { BoundedCapacity = 1 });

            target.Completion.ContinueWith(delegate
            {
                source.Complete();
            });

            return DataflowBlock.Encapsulate(target, source);
        }
    }
}