namespace Sitecore.Support.ContentSearch.Maintenance.Strategies
{
    using Sitecore.ContentSearch;
    using Sitecore.ContentSearch.Maintenance.Strategies.Models;
    using Sitecore.Data;
    using Sitecore.Data.Archiving;
    using Sitecore.Data.Eventing.Remote;
    using Sitecore.Diagnostics;
    using System.Collections.Generic;
    using System.Linq;
    using System;

    public class OnPublishEndAsynchronousStrategy : Sitecore.ContentSearch.Maintenance.Strategies.OnPublishEndAsynchronousStrategy
    {
        public OnPublishEndAsynchronousStrategy(string database) : base(database)
        {
        }

        protected override IndexableInfo[] PrepareIndexableInfo(List<IndexableInfoModel> data, long lastUpdatedStamp)
        {
            DataUriBucketDictionary<IndexableInfo> dataUriBucketDictionary = new DataUriBucketDictionary<IndexableInfo>();
            DataUriBucketDictionary<IndexableInfo> dataUriBucketDictionary2 = new DataUriBucketDictionary<IndexableInfo>();
            foreach (IndexableInfoModel datum in data)
            {
                if (datum.TimeStamp > lastUpdatedStamp)
                {
                    DataUri key = datum.Key;
                    IndexableInfo indexable = new IndexableInfo(datum.UniqueId, datum.TimeStamp);
                    if (datum.RemoteEvent is RemovedVersionRemoteEvent || datum.RemoteEvent is DeletedItemRemoteEvent)
                    {
                        HandleIndexableToRemove(dataUriBucketDictionary2, key, indexable);
                    }
                    else if (datum.RemoteEvent is AddedVersionRemoteEvent)
                    {
                        HandleIndexableToAddVersion(dataUriBucketDictionary, key, indexable);
                    }
                    else
                    {
                        // Sitecore.Support.335146+++
                        if (!IsValid(datum.RemoteEvent))
                        {
                            continue;
                        }
                        // Sitecore.Support.335146---

                        UpdateIndexableInfo(datum.RemoteEvent, indexable);
                        HandleIndexableToUpdate(dataUriBucketDictionary, key, indexable);
                    }
                }
            }
            return (from x in dataUriBucketDictionary.ExtractValues().Concat(dataUriBucketDictionary2.ExtractValues())
                    orderby x.Timestamp
                    select x).ToArray();
        }


        // Sitecore.Support.335146+++
        private bool IsValid(ItemRemoteEventBase remoteEvent)
        {
            if (remoteEvent is SavedItemRemoteEvent)
            {
                SavedItemRemoteEvent savedItemRemoteEvent = remoteEvent as SavedItemRemoteEvent;

                if (savedItemRemoteEvent.VersionNumber == 0)
                {
                    Sitecore.Diagnostics.Log.Debug("[Sitecore.Support.335146][OnPublishEndAsynchronousStrategy] SavedItemRemoteEvent is skipped as invalid:" + savedItemRemoteEvent.ItemName + ", " + savedItemRemoteEvent.ItemId + ", " + savedItemRemoteEvent.VersionNumber + ", " + savedItemRemoteEvent.LanguageName, this);

                    return false;
                }
            }

            return true;
        }
        // Sitecore.Support.335146---

        private void UpdateIndexableInfo(ItemRemoteEventBase instanceData, IndexableInfo indexable)
        {
            if (instanceData is SavedItemRemoteEvent)
            {
                SavedItemRemoteEvent obj = instanceData as SavedItemRemoteEvent;

                if (obj.IsSharedFieldChanged)
                {
                    indexable.IsSharedFieldChanged = true;
                }
                if (obj.IsUnversionedFieldChanged)
                {
                    indexable.IsUnversionedFieldChanged = true;
                }
            }
            if (instanceData is RestoreItemCompletedEvent)
            {
                indexable.IsSharedFieldChanged = true;
            }
            if (instanceData is CopiedItemRemoteEvent)
            {
                indexable.IsSharedFieldChanged = true;
                if ((instanceData as CopiedItemRemoteEvent).Deep)
                {
                    indexable.NeedUpdateChildren = true;
                }
            }
            MovedItemRemoteEvent movedItemRemoteEvent = instanceData as MovedItemRemoteEvent;
            if (movedItemRemoteEvent != null)
            {
                indexable.NeedUpdateChildren = true;
                indexable.OldParentId = movedItemRemoteEvent.OldParentId;
            }
        }

        private void HandleIndexableToRemove(DataUriBucketDictionary<IndexableInfo> collection, DataUri key, IndexableInfo indexable)
        {
            if (collection.ContainsKey(key))
            {
                collection[key].Timestamp = indexable.Timestamp;
            }
            else
            {
                collection.Add(key, indexable);
            }
        }

        private void HandleIndexableToAddVersion(DataUriBucketDictionary<IndexableInfo> collection, DataUri key, IndexableInfo indexable)
        {
            indexable.IsVersionAdded = true;
            if (!collection.ContainsKey(key))
            {
                collection.Add(key, indexable);
                return;
            }
            collection[key].IsVersionAdded = true;
            collection[key].Timestamp = indexable.Timestamp;
        }

        private void HandleIndexableToUpdate(DataUriBucketDictionary<IndexableInfo> collection, DataUri key, IndexableInfo indexable)
        {
            if (AlreadyAddedMovedItemEvent(collection, key.ItemID) || AlreadyAddedSharedFieldChange(collection, key.ItemID))
            {
                UpdateExistingItemInTheCollection(collection, key, indexable);
            }
            else if (indexable.IsSharedFieldChanged || indexable.NeedUpdateChildren)
            {
                collection.RemoveAll(key.ItemID);
                collection.Add(key, indexable);
            }
            else if (AlreadyAddedUnversionedFieldChange(collection, key))
            {
                collection.First(key.ItemID, (KeyValuePair<DataUri, IndexableInfo> x) => x.Key.Language == key.Language).Timestamp = indexable.Timestamp;
            }
            else if (indexable.IsUnversionedFieldChanged)
            {
                collection.RemoveAll(key.ItemID, (DataUri x) => x.Language == key.Language);
                collection.Add(key, indexable);
            }
            else if (collection.ContainsKey(key))
            {
                collection[key].Timestamp = indexable.Timestamp;
            }
            else
            {
                collection.Add(key, indexable);
            }
        }

        private bool AlreadyAddedMovedItemEvent(DataUriBucketDictionary<IndexableInfo> collection, ID id)
        {
            return collection.ContainsAny(id, (KeyValuePair<DataUri, IndexableInfo> x) => x.Value.NeedUpdateChildren);
        }

        private bool AlreadyAddedSharedFieldChange(DataUriBucketDictionary<IndexableInfo> collection, ID id)
        {
            return collection.ContainsAny(id, (KeyValuePair<DataUri, IndexableInfo> x) => x.Value.IsSharedFieldChanged);
        }

        private bool AlreadyAddedUnversionedFieldChange(DataUriBucketDictionary<IndexableInfo> collection, DataUri key)
        {
            return collection.ContainsAny(key.ItemID, (KeyValuePair<DataUri, IndexableInfo> x) => x.Key.Language == key.Language && x.Value.IsUnversionedFieldChanged);
        }


        private void UpdateExistingItemInTheCollection(DataUriBucketDictionary<IndexableInfo> collection, DataUri key, IndexableInfo indexable)
        {
            Assert.IsNotNull(collection, "collection");
            Assert.IsNotNull(indexable, "indexable");
            Assert.IsNotNull(key, "key");
            IndexableInfo indexableInfo = collection.First(key.ItemID);
            Assert.IsNotNull(indexableInfo, "originalEntry");
            IIndexableUniqueId indexableUniqueId = indexableInfo.IndexableUniqueId;
            Assert.IsNotNull(indexableUniqueId, "oriEntryindexableUniqueId");
            ItemUri itemUri = indexableUniqueId.Value as ItemUri;
            Assert.IsNotNull(itemUri, "originalEntryItemUri");
            if (key.Version.Number > itemUri.Version.Number)
            {
                indexable = MergingIndexableInfo(indexableInfo, indexable);
                collection.RemoveAll(key.ItemID);
                collection.Add(key, indexable);
            }
            else
            {
                indexableInfo.Timestamp = indexable.Timestamp;
                indexableInfo.NeedUpdateChildren = (indexableInfo.NeedUpdateChildren || indexable.NeedUpdateChildren);
            }
        }
        private IndexableInfo MergingIndexableInfo(IndexableInfo from, IndexableInfo into)
        {
            into.IsVersionAdded = (from.IsVersionAdded || into.IsVersionAdded);
            into.IsSharedFieldChanged = (from.IsSharedFieldChanged || into.IsSharedFieldChanged);
            into.IsUnversionedFieldChanged = (from.IsUnversionedFieldChanged || into.IsUnversionedFieldChanged);
            into.NeedUpdateChildren = (from.NeedUpdateChildren || into.NeedUpdateChildren);
            return into;
        }
    }
}