using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Azure.EntityServices.Tables.Core.Abstractions;

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
        public static IEntityTransactionGroupPipeline CreatePipeline(
            Func<EntityTransactionGroup, Task<EntityTransactionGroup>> asyncPreProcessor,
            Func<EntityTransactionGroup[],
            Task> asyncProcessor,
            int maxItemInBatch,
            int maxItemInTransaction,
            int maxParallelTasks)
        {
            var preProcessorBlock = new TransformBlock<EntityTransactionGroup, EntityTransactionGroup>(transactions =>
            {
                return asyncPreProcessor(transactions);

            },
              new ExecutionDataflowBlockOptions()
              {
                  BoundedCapacity = maxParallelTasks,
                  MaxDegreeOfParallelism = maxParallelTasks
              });
            //Create en configure transaction entity group flow pipepline
            var batchingBlock = new BatchBlock<EntityTransactionGroup>(maxItemInBatch, new GroupingDataflowBlockOptions()
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

            var processorBlock = new ActionBlock<EntityTransactionGroup[]>(async (p) =>
            {
                try
                {
                    await asyncProcessor(p);
                }
                catch (Exception ex)
                {
                    if (preProcessorBlock.Completion.IsCompleted)
                    {
                        throw;
                    }
                    //to prevent pipeline to be blocked, the pipeline must be completed manually with a faulted state
                     (preProcessorBlock as IDataflowBlock)?.Fault(ex);
                }
            },
                new ExecutionDataflowBlockOptions()
                {
                    BoundedCapacity = maxParallelTasks,
                    MaxDegreeOfParallelism = maxParallelTasks
                });

            //link blocks together
            preProcessorBlock.LinkTo(batchingBlock, new DataflowLinkOptions() { PropagateCompletion = true });
            batchingBlock.LinkTo(groupPerPartitionsBlock, new DataflowLinkOptions() { PropagateCompletion = true });
            groupPerPartitionsBlock.LinkTo(transactionGroupBlock, new DataflowLinkOptions() { PropagateCompletion = true });
            transactionGroupBlock.LinkTo(processorBlock, new DataflowLinkOptions() { PropagateCompletion = true });

            return new EntityTransactionGroupPipeline(preProcessorBlock, processorBlock);
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
            }, new ExecutionDataflowBlockOptions
            {
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