namespace Sitecore.Support.ContentSearch.Maintenance.Strategies
{
    using Sitecore.ContentSearch;
    using Sitecore.ContentSearch.Diagnostics;
    using Sitecore.ContentSearch.Maintenance.Strategies.Models;
    using Sitecore.Eventing;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;

    public class OnPublishEndAsynchronousSingleInstanceStrategy : OnPublishEndAsynchronousStrategy
    {


        public OnPublishEndAsynchronousSingleInstanceStrategy(string database) : base(database)
        {
        }

        public override void Run()
        {
            EventManager.RaiseQueuedEvents();
            EventQueue queue = base.Database.RemoteEvents.Queue;
            if (queue == null)
            {
                CrawlingLog.Log.Fatal($"Event Queue is empty. Returning.");
                return;
            }
            ISearchIndex searchIndex = (from index in base.Indexes
                                        orderby index.Summary.LastUpdatedTimestamp ?? 0
                                        select index).FirstOrDefault();
            long? lastUpdatedTimestamp = 0L;
            if (searchIndex != null)
            {
                lastUpdatedTimestamp = searchIndex.Summary.LastUpdatedTimestamp;
            }
            List<QueuedEvent> queue2 = ReadQueue(queue, lastUpdatedTimestamp);
            List<IndexableInfoModel> data = PrepareIndexData(queue2, base.Database);
            if (base.ContentSearchSettings.IsParallelIndexingEnabled())
            {
                ParallelForeachProxy.ForEach(base.Indexes, new ParallelOptions
                {
                    TaskScheduler = TaskSchedulerManager.GetLimitedConcurrencyLevelTaskSchedulerForIndexing(base.Indexes.Count + 1)
                }, delegate (ISearchIndex index)
                {
                    base.Run(data, index);
                });
            }
            else
            {
                foreach (ISearchIndex index in base.Indexes)
                {
                    base.Run(data, index);
                }
            }
        }
    }
}