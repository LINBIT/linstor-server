package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.backupshipping.BackupShippingUtils;
import com.linbit.linstor.core.CoreModule.ResourceDefinitionMap;
import com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.data.BackupShippingDstData;
import com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.data.BackupShippingSrcData;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.remotes.AbsRemote;
import com.linbit.linstor.core.objects.remotes.StltRemote;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.tasks.StltRemoteCleanupTask;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.utils.BidirectionalMultiMap;
import com.linbit.utils.PairNonNull;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Consumer;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

@Singleton
public class BackupInfoManager
{
    private final Map<ResourceDefinition, String> restoreMap;
    private final Map<NodeName, Map<SnapshotDefinition.Key, AbortInfo>> abortCreateMap;
    private final Map<ResourceName, Set<Snapshot>> abortRestoreMap;
    private final Map<Snapshot, Snapshot> backupsToDownload;

    private final Map<RemoteName, Set<SnapshotDefinition>> inProgressBackups;

    private final Object restoreSyncObj = new Object();
    // Map<LinstorRemoteName, Map<StltRemoteName, Data>>
    private final Map<RemoteName, Map<RemoteName, BackupShippingSrcData>> l2lSrcData;
    private final Map<Snapshot, BackupShippingDstData> l2lDstData;
    private final BidirectionalMultiMap<Node, QueueItem> uploadQueues;
    /*
     * set of queueItems whose prevSnap is missing the info which node it was shipped by
     * since QueueItems are comparedTo using an internal (creation-) timestamp, this set can be viewed as a queue of
     * uploadQueues with no node associated
     */
    // uses uploadQueues as sync-object
    private final TreeSet<QueueItem> prevNodeUndecidedQueue;
    private final Map<StltRemote, CleanupData> cleanupDataMap;
    private final Map<PairNonNull<SnapshotDefinition, String>, List<FluxSink<ApiCallRc>>> waitForSnapSentMap;
    private final Map<Snapshot, List<FluxSink<ApiCallRc>>> waitForSnapReceiveMap;

    private final AccessContext sysCtx;
    private final ErrorReporter errorReporter;
    private final ResourceDefinitionMap rscDfnMap;

    @Inject
    public BackupInfoManager(
        TransactionObjectFactory transObjFactoryRef,
        @SystemContext AccessContext sysCtxRef,
        ErrorReporter errorReporterRef,
        ResourceDefinitionMap rscDfnMapRef
    )
    {
        sysCtx = sysCtxRef;
        errorReporter = errorReporterRef;
        rscDfnMap = rscDfnMapRef;
        restoreMap = transObjFactoryRef.createTransactionPrimitiveMap(null, new HashMap<>(), null);
        abortCreateMap = new HashMap<>();
        abortRestoreMap = new HashMap<>();
        backupsToDownload = new HashMap<>();
        inProgressBackups = new HashMap<>();
        l2lSrcData = new HashMap<>();
        l2lDstData = new HashMap<>();
        uploadQueues = new BidirectionalMultiMap<>();
        prevNodeUndecidedQueue = new TreeSet<>();
        cleanupDataMap = new HashMap<>();
        waitForSnapSentMap = new HashMap<>();
        waitForSnapReceiveMap = new HashMap<>();
    }

    public boolean addAllRestoreEntries(
        ResourceDefinition rscDfn,
        String metaName,
        String rscNameStr,
        List<Snapshot> snaps,
        Map<Snapshot, Snapshot> snapsToDownload,
        RemoteName remoteName
    )
    {
        synchronized (restoreSyncObj)
        {
            boolean newShipping = restoreAddEntry(rscDfn, metaName);
            boolean addedSuccessfully = true;
            if (newShipping)
            {
                for (Snapshot snap : snaps)
                {
                    abortRestoreAddEntry(rscNameStr, snap);
                    addSnapToInProgressBackups(remoteName, snap.getSnapshotDefinition());
                }
                for (Entry<Snapshot, Snapshot> toDownload : snapsToDownload.entrySet())
                {
                    addedSuccessfully = backupsToDownloadAddEntry(toDownload.getKey(), toDownload.getValue());
                    if (!addedSuccessfully)
                    {
                        break;
                    }
                }
            }
            return newShipping && addedSuccessfully;
        }
    }

    public Set<RemoteName> removeAllRestoreEntries(ResourceDefinition rscDfn, String rscName, Snapshot snap)
    {
        synchronized (restoreSyncObj)
        {
            restoreRemoveEntry(rscDfn);
            abortRestoreDeleteAllEntries(rscName);
            backupsToDownloadCleanUp(snap);
            return removeInProgressBackups(Collections.singleton(snap.getSnapshotDefinition()));
        }
    }

    public PairNonNull<Set<SnapshotDefinition>, Set<RemoteName>> removeAllRestoreEntries(
        AccessContext accCtx,
        Node node
    )
        throws AccessDeniedException
    {
        Set<SnapshotDefinition> snapDfnsToCleanup = new HashSet<>();
        synchronized (restoreSyncObj)
        {
            Iterator<Snapshot> localSnapIter = node.iterateSnapshots(accCtx);
            while (localSnapIter.hasNext())
            {
                Snapshot localSnap = localSnapIter.next();
                @Nullable Props snapDfnTargetProps = localSnap.getSnapshotDefinition()
                    .getSnapDfnProps(accCtx)
                    .getNamespace(BackupShippingUtils.BACKUP_TARGET_PROPS_NAMESPC);
                if (snapDfnTargetProps != null)
                {
                    String targetStatus = snapDfnTargetProps.getProp(InternalApiConsts.KEY_SHIPPING_STATUS);
                    if (targetStatus != null && targetStatus.equals(InternalApiConsts.VALUE_SHIPPING))
                    {
                        snapDfnsToCleanup.add(localSnap.getSnapshotDefinition());
                        // complete abort, remove restore-lock on rscDfn
                        restoreRemoveEntry(localSnap.getResourceDefinition());
                        abortRestoreDeleteAllEntries(localSnap.getResourceName().displayValue);
                        backupsToDownloadCleanUp(localSnap);
                    }
                }
            }
            Set<RemoteName> remotesToCleanup = removeInProgressBackups(snapDfnsToCleanup);
            return new PairNonNull<>(snapDfnsToCleanup, remotesToCleanup);
        }
    }

    /**
     * mark a rscDfn as target of a backup restore. rscDfns in this map should not be modified in any way
     * also add the backup that is the source of the restore, to avoid multiple restores
     * of the same backup at the same time
     */
    private boolean restoreAddEntry(ResourceDefinition rscDfn, String metaName)
    {
        boolean addFlag = !restoreMap.containsKey(rscDfn);
        if (addFlag)
        {
            restoreMap.put(rscDfn, metaName);
        }
        return addFlag;
    }

    /**
     * unmark the rscDfn to signify the backup restore is done and allow other modifications to take place
     * and also free the source backup for the next restore
     */
    private void restoreRemoveEntry(ResourceDefinition rscDfn)
    {
        restoreMap.remove(rscDfn);
    }

    /**
     * check if a rscDfn has been marked as a target of a restore
     */
    public boolean restoreContainsRscDfn(ResourceDefinition rscDfn)
    {
        synchronized (restoreSyncObj)
        {
            return restoreMap.containsKey(rscDfn);
        }
    }

    /**
     * check if a certain backup is currently being restored
     */
    public boolean restoreContainsMetaFile(String metaName)
    {
        synchronized (restoreSyncObj)
        {
            return restoreMap.containsValue(metaName);
        }
    }

    /**
     * abortRestore saves a list of snapshots used in a restore for each rscDfn that need to be
     * taken care of in case of an abort. This method adds a snapshot to that list
     */
    private void abortRestoreAddEntry(String rscNameStr, Snapshot snap)
    {
        try
        {
            ResourceName rscName = new ResourceName(rscNameStr);
            Set<Snapshot> snaps = abortRestoreMap.computeIfAbsent(rscName, k -> new HashSet<>());
            snaps.add(snap);
        }
        catch (InvalidNameException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    /**
     * get a copy of all the restore-related snapshots that need to be aborted of a specific rscDfn
     */
    public @Nullable Set<Snapshot> abortRestoreGetEntries(String rscNameStr)
    {
        try
        {
            synchronized (restoreSyncObj)
            {
                ResourceName rscName = new ResourceName(rscNameStr);
                Set<Snapshot> ret = abortRestoreMap.get(rscName);
                return ret != null ? new HashSet<>(ret) : null;
            }
        }
        catch (InvalidNameException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    /**
     * remove the rscDfn and all the remaining snapshots in the list, signifying that the restore or abort is done
     */
    private void abortRestoreDeleteAllEntries(String rscNameStr)
    {
        try
        {
            ResourceName rscName = new ResourceName(rscNameStr);
            abortRestoreMap.remove(rscName);
        }
        catch (InvalidNameException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    /**
     * remove a single snapshot from the list associated with the given rscDfn,
     * signifying that this snapshot is completed and no longer needs aborting
     *
     * @return set of remoteNames from {@link #removeInProgressBackups(Set)}
     */
    public Set<RemoteName> abortRestoreDeleteEntry(String rscNameStr, Snapshot snap)
    {
        try
        {
            synchronized (restoreSyncObj)
            {
                ResourceName rscName = new ResourceName(rscNameStr);
                Set<Snapshot> snaps = abortRestoreMap.get(rscName);
                if (snaps != null)
                {
                    snaps.remove(snap);
                }
                return removeInProgressBackups(Collections.singleton(snap.getSnapshotDefinition()));
            }
        }
        catch (InvalidNameException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    /**
     * add all the information needed to cleanly abort a multipart-upload to s3 to a list for easy access when needed
     */
    public void abortCreateAddS3Entry(
        NodeName nodeName,
        ResourceName rscName,
        SnapshotName snapName,
        String backupName,
        String uploadId,
        RemoteName remoteName
    )
    {
        // DO NOT just synchronize within getAbortInfo as the following .add(new Abort*Info(..)) should also be in the
        // same synchronized block as the initial get
        synchronized (abortCreateMap)
        {
            try
            {
                SnapshotDefinition snapDfn = rscDfnMap.get(rscName).getSnapshotDfn(sysCtx, snapName);
                addSnapToInProgressBackups(remoteName, snapDfn);
                getAbortCreateInfo(nodeName, snapDfn.getSnapDfnKey()).abortS3InfoList.add(
                    new AbortS3Info(backupName, uploadId, remoteName.displayValue)
                );
            }
            catch (AccessDeniedException exc)
            {
                throw new ImplementationError(exc);
            }
        }
    }

    /**
     * add information about a l2l-shipping to a list to automatically abort it when issues like loss of connection
     * arise
     */
    public void abortCreateAddL2LEntry(NodeName nodeName, SnapshotDefinition.Key key, RemoteName remoteName)
    {
        // DO NOT just synchronize within getAbortInfo as the following .add(new Abort*Info(..)) should also be in the
        // same synchronized block as the initial get
        synchronized (abortCreateMap)
        {
            SnapshotDefinition snapDfn;
            try
            {
                snapDfn = rscDfnMap.get(key.getResourceName()).getSnapshotDfn(sysCtx, key.getSnapshotName());
                addSnapToInProgressBackups(remoteName, snapDfn);
                getAbortCreateInfo(nodeName, key).abortL2LInfoList.add(new AbortL2LInfo());
            }
            catch (AccessDeniedException exc)
            {
                throw new ImplementationError(exc);
            }
        }
    }

    private AbortInfo getAbortCreateInfo(NodeName nodeName, SnapshotDefinition.Key snapDfnKey)
    {
        Map<SnapshotDefinition.Key, AbortInfo> map = abortCreateMap.computeIfAbsent(nodeName, k -> new HashMap<>());
        return map.computeIfAbsent(snapDfnKey, a -> new AbortInfo());
    }

    /**
     * delete the abort-information given when it is no longer needed
     *
     * @return set of remoteNames from {@link #removeInProgressBackups(Set)}
     */
    public Set<RemoteName> abortCreateDeleteEntries(String nodeName, String rscName, String snapName)
        throws InvalidNameException
    {
        synchronized (abortCreateMap)
        {
            return abortCreateDeleteEntries(
                new NodeName(nodeName),
                new SnapshotDefinition.Key(new ResourceName(rscName), new SnapshotName(snapName))
            );
        }
    }

    /**
     * delete the abort-information given when it is no longer needed
     *
     * @return set of remoteNames from {@link #removeInProgressBackups(Set)}
     */
    public Set<RemoteName> abortCreateDeleteEntries(NodeName nodeName, SnapshotDefinition.Key snapDfnKey)
    {
        synchronized (abortCreateMap)
        {
            Map<SnapshotDefinition.Key, AbortInfo> map = abortCreateMap.get(nodeName);
            if (map != null)
            {
                map.remove(snapDfnKey);
            }
            try
            {
                return removeInProgressBackups(
                    Collections.singleton(
                        rscDfnMap.get(snapDfnKey.getResourceName()).getSnapshotDfn(sysCtx, snapDfnKey.getSnapshotName())
                    )
                );
            }
            catch (AccessDeniedException exc)
            {
                throw new ImplementationError(exc);
            }
        }
    }

    /**
     * get a copy of the abort-information to use it
     */
    public @Nullable Map<SnapshotDefinition.Key, AbortInfo> abortCreateGetEntries(NodeName nodeName)
    {
        synchronized (abortCreateMap)
        {
            Map<SnapshotDefinition.Key, AbortInfo> ret = abortCreateMap.get(nodeName);
            return ret != null ? new HashMap<>(ret) : null;
        }
    }

    /**
     * add a pair of snapshots, with the second snapshot being the first snapshot's successor
     * these are used to determine which backups need to be downloaded during a restore
     */
    private boolean backupsToDownloadAddEntry(Snapshot snap, Snapshot successor)
    {
        boolean addFlag;
        addFlag = !backupsToDownload.containsKey(snap);
        if (addFlag)
        {
            backupsToDownload.put(snap, successor);
        }
        return addFlag;
    }

    /**
     * get the successor of the given snapshot and delete the entry
     */
    public @Nullable Snapshot getNextBackupToDownload(Snapshot snap)
    {
        synchronized (restoreSyncObj)
        {
            return backupsToDownload.remove(snap);
        }
    }

    /**
     * clean up the download-map when the restore is aborted by anything
     */
    private void backupsToDownloadCleanUp(Snapshot snap)
    {
        Snapshot toDelete = backupsToDownload.remove(snap);
        while (toDelete != null)
        {
            toDelete = backupsToDownload.remove(toDelete);
        }
    }

    /**
     * checks if the given remote is currently used for any backup (create or restore)
     *
     * @param remote
     *
     * @return
     */
    public boolean hasRemoteInProgressBackups(RemoteName remote)
    {
        synchronized (inProgressBackups)
        {
            Set<SnapshotDefinition> snaps = inProgressBackups.get(remote);
            return snaps != null && !snaps.isEmpty();
        }
    }

    /**
     * add the given snapDfn to mark that the given remote is currently used for this backup
     *
     * @param remote
     * @param snapDfn
     */
    private void addSnapToInProgressBackups(RemoteName remote, SnapshotDefinition snapDfn)
    {
        synchronized (inProgressBackups)
        {
            inProgressBackups.computeIfAbsent(remote, ignored -> new HashSet<>()).add(snapDfn);
        }
    }

    /**
     * removes the given snapDfns from the inProgressBackups map, and returns a set of all remoteNames that now have no
     * in-progress backups (aka need to be checked if they need to be deleted)
     *
     * @param snapDfnsToCleanup
     *
     * @return
     */
    private Set<RemoteName> removeInProgressBackups(Set<SnapshotDefinition> snapDfnsToCleanup)
    {
        synchronized (inProgressBackups)
        {
            Set<RemoteName> emptyRemotes = new HashSet<>();
            for (Entry<RemoteName, Set<SnapshotDefinition>> entry : inProgressBackups.entrySet())
            {
                Set<SnapshotDefinition> snapDfns = entry.getValue();
                snapDfns.removeAll(snapDfnsToCleanup);
                if (snapDfns.isEmpty())
                {
                    emptyRemotes.add(entry.getKey());
                }
            }
            for (RemoteName emptyRemote : emptyRemotes)
            {
                inProgressBackups.remove(emptyRemote);
            }
            return emptyRemotes;
        }
    }

    public void addL2LSrcData(
        RemoteName linstorRemoteName,
        RemoteName stltRemoteName,
        BackupShippingSrcData data
    )
    {
        synchronized (l2lSrcData)
        {
            l2lSrcData.computeIfAbsent(
                linstorRemoteName,
                ignore -> new HashMap<>()
            ).put(stltRemoteName, data);
        }
    }

    public BackupShippingSrcData getL2LSrcData(
        RemoteName linstorRemoteName,
        RemoteName stltRemoteName
    )
    {
        synchronized (l2lSrcData)
        {
            return l2lSrcData.get(linstorRemoteName).get(stltRemoteName);
        }
    }

    public @Nullable BackupShippingSrcData removeL2LSrcData(
        RemoteName linstorRemoteName,
        RemoteName stltRemoteName
    )
    {
        BackupShippingSrcData ret;
        synchronized (l2lSrcData)
        {
            Map<RemoteName, BackupShippingSrcData> innerMap = l2lSrcData.get(linstorRemoteName);
            if (innerMap != null)
            {
                ret = innerMap.remove(stltRemoteName);
                if (innerMap.isEmpty())
                {
                    l2lSrcData.remove(linstorRemoteName);
                }
            }
            else
            {
                ret = null;
            }
        }
        return ret;
    }

    public void addL2LDstData(Snapshot snap, BackupShippingDstData data)
    {
        l2lDstData.put(snap, data);
    }

    public BackupShippingDstData getL2LDstData(Snapshot snap)
    {
        return l2lDstData.get(snap);
    }

    public void removeL2LDstData(Snapshot snap)
    {
        l2lDstData.remove(snap);
    }

    public Set<Entry<QueueItem, Set<Node>>> getSnapToNodeQueues()
    {
        synchronized (uploadQueues)
        {
            return uploadQueues.entrySetInverted();
        }
    }

    public Set<Entry<Node, Set<QueueItem>>> getNodeToSnapQueues()
    {
        synchronized (uploadQueues)
        {
            return uploadQueues.entrySet();
        }
    }

    public Set<QueueItem> getPrevNodeUndecidedQueue()
    {
        synchronized (uploadQueues)
        {
            return Collections.unmodifiableSet(prevNodeUndecidedQueue);
        }
    }

    /**
     * Add the snapDfn to the queues of all nodes in usableNodes
     * If usableNodes is empty or null, it will be added to the prevNodeUndecidedQueue
     */
    public void addToQueues(
        SnapshotDefinition snapDfn,
        AbsRemote remote,
        @Nullable SnapshotDefinition prevSnapDfn,
        @Nullable String preferredNode,
        @Nullable BackupShippingSrcData l2lData,
        @Nullable Set<Node> usableNodes,
        boolean shipExistingSnap
    )
    {
        synchronized (uploadQueues)
        {
            QueueItem item = new QueueItem(snapDfn, remote, prevSnapDfn, preferredNode, l2lData, shipExistingSnap);
            if (usableNodes != null && !usableNodes.isEmpty())
            {
                for (Node node : usableNodes)
                {
                    uploadQueues.add(node, item);
                }
            }
            else
            {
                prevNodeUndecidedQueue.add(item);
            }
        }
    }

    public boolean isSnapshotQueued(SnapshotDefinition snapDfn)
    {
        synchronized (uploadQueues)
        {
            return setContainsSnapDfn(snapDfn, prevNodeUndecidedQueue) ||
                setContainsSnapDfn(snapDfn, uploadQueues.valueSet());
        }
    }

    public boolean hasNodeQueuedSnaps(Node node)
    {
        synchronized (uploadQueues)
        {
            return uploadQueues.containsKey(node);
        }
    }

    private boolean setContainsSnapDfn(SnapshotDefinition snapDfn, Set<QueueItem> set)
    {
        boolean ret = false;
        for (QueueItem item : set)
        {
            if (item.snapDfn.equals(snapDfn))
            {
                ret = true;
                break;
            }
        }
        return ret;
    }

    /**
     * Return the next snapDfn from the queue of the given node and remove it from all queues it was in (including the
     * given node's)
     */
    public @Nullable QueueItem getNextFromQueue(AccessContext accCtx, Node node, boolean consume)
        throws AccessDeniedException
    {
        QueueItem ret = null;
        synchronized (uploadQueues)
        {
            Set<QueueItem> queue = uploadQueues.getByKey(node);
            if (queue != null && !queue.isEmpty())
            {
                Iterator<QueueItem> iterator = queue.iterator();
                while (iterator.hasNext() && ret == null)
                {
                    QueueItem next = iterator.next();
                    SnapshotDefinition prevSnapDfn = next.prevSnapDfn;
                    boolean isValid = prevSnapDfn == null || prevSnapDfn.isDeleted() ||
                        BackupShippingUtils.hasShippingStatus(
                            prevSnapDfn,
                            next.s3orLinRemote.getName().displayValue,
                            InternalApiConsts.VALUE_SUCCESS,
                            accCtx
                        );
                    if (isValid)
                    {
                        ret = next;
                    }
                }
                if (ret != null && consume)
                {
                    // make sure this snapDfn gets removed from all queues so that the shipping is only started once
                    uploadQueues.removeValue(ret);
                }
            }
        }
        return ret;
    }

    public void deleteFromQueue(SnapshotDefinition snapDfn, AbsRemote remote)
    {
        synchronized (uploadQueues)
        {
            // this works because hashCode & equals only use snapDfn & remote and ignore the prevSnapDfn
            QueueItem toDelete = new QueueItem(snapDfn, remote, null, null, null, false);
            uploadQueues.removeValue(toDelete);
            prevNodeUndecidedQueue.remove(toDelete);
        }
    }

    /**
     * Deletes all entries that require the given remote
     */
    public void deleteFromQueue(AbsRemote remote)
    {
        synchronized (uploadQueues)
        {
            for (Entry<Node, Set<QueueItem>> queue : uploadQueues.entrySet())
            {
                Set<QueueItem> itemsToDelete = getItemsByRemote(remote, queue.getValue());
                for (QueueItem toDelete : itemsToDelete)
                {
                    uploadQueues.removeValue(toDelete);
                }
            }
            Set<QueueItem> itemsToDelete = getItemsByRemote(remote, prevNodeUndecidedQueue);
            prevNodeUndecidedQueue.removeAll(itemsToDelete);
        }
    }

    private Set<QueueItem> getItemsByRemote(AbsRemote remote, Set<QueueItem> items)
    {
        Set<QueueItem> ret = new HashSet<>();
        for (QueueItem item : items)
        {
            if (item.s3orLinRemote.equals(remote))
            {
                ret.add(item);
            }
        }
        return ret;
    }

    /**
     * Deletes the queue of this node
     *
     * @return A list of all backups that need to be queued somewhere else because this node was
     * the last queue they were a part of
     */
    public List<QueueItem> deleteFromQueue(Node node)
    {
        synchronized (uploadQueues)
        {
            List<QueueItem> ret = new ArrayList<>();
            // removes the node from all item-lists, and if the list is empty afterwards, deletes the item as well
            Set<QueueItem> itemsToCheck = uploadQueues.removeKey(node);
            if (itemsToCheck != null)
            {
                for (QueueItem item : itemsToCheck)
                {
                    if (!uploadQueues.containsValue(item))
                    {
                        // the backup is not queued anywhere anymore and new nodes need to be chosen for it
                        ret.add(item);
                    }
                }
            }
            return ret;
        }
    }

    /**
     * Deletes ALL queues
     */
    public void deleteAllQueues()
    {
        synchronized (uploadQueues)
        {
            uploadQueues.clear();
            prevNodeUndecidedQueue.clear();
        }
    }

    public void deletePrevSnapFromQueueItems(SnapshotDefinition prevSnapDfn)
    {
        synchronized (uploadQueues)
        {
            for (Entry<QueueItem, Set<Node>> entry : uploadQueues.entrySetInverted())
            {
                QueueItem item = entry.getKey();
                if (prevSnapDfn.equals(item.prevSnapDfn))
                {
                    // set prevSnapDfn to null, forcing a full backup. Trying to get a new incremental base might be
                    // possible for s3, but would probably not work for l2l
                    item.prevSnapDfn = null;
                }
            }
        }
    }

    public @Nullable QueueItem getItemFromPrevNodeUndecidedQueue(SnapshotDefinition snapDfn, AbsRemote remote)
    {
        synchronized (uploadQueues)
        {
            QueueItem ret = null;
            for (QueueItem item : prevNodeUndecidedQueue)
            {
                // this list SHOULD NOT contain items where prevSnapDfn == null
                final @Nullable SnapshotDefinition prevSnap = item.prevSnapDfn;
                if (prevSnap != null)
                {
                    if (!prevSnap.isDeleted() && prevSnap.equals(snapDfn) && item.s3orLinRemote.equals(remote))
                    {
                        ret = item;
                        break;
                    }
                }
                else
                {
                    throw new ImplementationError(
                        "prevNodeUndecidedQueue is not allowed to contain items where prevSnapDfn is null"
                    );
                }
            }
            if (ret != null)
            {
                prevNodeUndecidedQueue.remove(ret);
            }
            return ret;
        }
    }

    public Map<QueueItem, TreeSet<Node>> getFollowUpSnaps(SnapshotDefinition snapDfn, AbsRemote remote)
    {
        synchronized (uploadQueues)
        {
            /*
             * Does not need to consider prevNodeUndecidedQueue since that queue can only contain snaps whose prevSnap
             * has yet to start shipping, whereas the snaps we are looking for have the snap that just finished shipping
             * as their prevSnap
             */
            Map<QueueItem, TreeSet<Node>> ret = new TreeMap<>();
            for (Entry<QueueItem, Set<Node>> entry : uploadQueues.entrySetInverted())
            {
                QueueItem item = entry.getKey();
                // TODO: for optimizing when snaps get started, the isDeleted check in this method is a good starting
                // point, since those that trigger this check will never be started by a getFollowUpSnaps call.
                SnapshotDefinition prevSnapDfn = item.prevSnapDfn;
                if (
                    item.s3orLinRemote.equals(remote) && prevSnapDfn != null &&
                        !prevSnapDfn.isDeleted() && prevSnapDfn.equals(snapDfn)
                )
                {
                    ret.put(item, new TreeSet<>(entry.getValue()));
                }
            }
            return ret;
        }
    }

    public void addCleanupData(BackupShippingSrcData data)
    {
        synchronized (cleanupDataMap)
        {
            cleanupDataMap.put(data.getStltRemote(), new CleanupData(data));
        }
    }

    public void addTaskToCleanupData(StltRemote remote, StltRemoteCleanupTask task)
    {
        synchronized (cleanupDataMap)
        {
            CleanupData cleanupData = cleanupDataMap.get(remote);
            if (cleanupData != null)
            {
                cleanupData.task = task;
            }
        }
    }

    /**
     * Returns null if cleanup can not be started
     * if srcSuccessRef is false, this method will ignore the finishedCount and return the cleanup data anyways
     */
    public @Nullable CleanupData l2lShippingFinished(StltRemote remote, boolean srcSuccessRef)
    {
        synchronized (cleanupDataMap)
        {

            CleanupData cleanupData = null;
            if (!remote.isDeleted())
            {
                cleanupData = cleanupDataMap.get(remote);
            }
            if (cleanupData != null)
            {
                cleanupData.finishedCount++;
                boolean startCleanup = cleanupData.finishedCount == 2 || !srcSuccessRef;
                if (startCleanup)
                {
                    cleanupDataMap.remove(remote);
                }
                else
                {
                    cleanupData = null;
                }
            }
            return cleanupData;
        }
    }

    public Flux<ApiCallRc> registerWaitForShipSentDoneFlux(SnapshotDefinition snapDfnRef, String remoteRef)
    {
        return registerWaitForShipDoneFlux(waitForSnapSentMap, new PairNonNull<>(snapDfnRef, remoteRef));
    }

    public Flux<ApiCallRc> registerWaitForShipReceiveDoneFlux(Snapshot snapRef)
    {
        return registerWaitForShipDoneFlux(waitForSnapReceiveMap, snapRef);
    }

    private <KEY> Flux<ApiCallRc> registerWaitForShipDoneFlux(
        Map<KEY, List<FluxSink<ApiCallRc>>> fluxSinkMap,
        KEY keyRef
    )
    {
        return Flux.create(sink ->
        {
            // dummy snycObject just to keep code analyzers silent about "not syncing on parameters.
            // this "map" parameter is always a private class variable, so this class has full control over it
            final Object syncObj = fluxSinkMap;
            synchronized (syncObj)
            {
                sink.onDispose(() ->
                {
                    synchronized (syncObj)
                    {
                        @Nullable List<FluxSink<ApiCallRc>> list = fluxSinkMap.get(keyRef);
                        if (list != null)
                        {
                            list.remove(sink);
                            if (list.isEmpty())
                            {
                                fluxSinkMap.remove(keyRef);
                            }
                        }
                    }
                });
                fluxSinkMap.computeIfAbsent(keyRef, ignored -> new ArrayList<>())
                    .add(sink);
            }
        });
    }

    public Flux<ApiCallRc> completeWaitForShipSentDoneFlux(SnapshotDefinition snapDfnRef, String remoteNameRef)
    {
        return completeWaitFlux(waitForSnapSentMap, new PairNonNull<>(snapDfnRef, remoteNameRef));
    }
    public Flux<ApiCallRc> completeWaitForShipReceiveDoneFlux(Snapshot snapRef)
    {
        return completeWaitFlux(waitForSnapReceiveMap, snapRef);
    }

    public Flux<ApiCallRc> errorWaitForShipSentDoneFlux(SnapshotDefinition snapDfnRef, String remoteNameRef)
    {
        return errorWaitForFlux(waitForSnapSentMap, new PairNonNull<>(snapDfnRef, remoteNameRef));
    }

    public Flux<ApiCallRc> errorWaitForShipReceiveDoneFlux(Snapshot snapRef)
    {
        return errorWaitForFlux(waitForSnapReceiveMap, snapRef);
    }

    private <KEY> Flux<ApiCallRc> errorWaitForFlux(Map<KEY, List<FluxSink<ApiCallRc>>> fluxSinkMap, KEY keyRef)
    {
        return finishWaitingSink(
            fluxSinkMap,
            keyRef,
            sink -> sink.error(
                new ShipmentFailedException(
                    "shipment of snapshot " + keyRef + " during node evacuate failed"
                )
            )
        );
    }

    private <KEY> Flux<ApiCallRc> completeWaitFlux(Map<KEY, List<FluxSink<ApiCallRc>>> fluxSinkMap, KEY keyRef)
    {
        return finishWaitingSink(
            fluxSinkMap,
            keyRef,
            FluxSink::complete
        );
    }

    private <KEY> Flux<ApiCallRc> finishWaitingSink(
        Map<KEY, List<FluxSink<ApiCallRc>>> fluxSinkMap,
        KEY keyRef,
        Consumer<FluxSink<ApiCallRc>> consumerRef
    )
    {
        Flux<ApiCallRc> ret;
        // dummy snycObject just to keep code analyzers silent about "not syncing on parameters.
        // this "map" parameter is always a private class variable, so this class has full control over it
        final Object syncObj = fluxSinkMap;
        synchronized (syncObj)
        {
            @Nullable List<FluxSink<ApiCallRc>> sinkList = fluxSinkMap.remove(keyRef);
            if (sinkList != null)
            {
                ret = Flux.create(innerSink ->
                {
                    // we have a race-condition while evacuating that the shipment is sent but not
                    // fully finished (i.e. the BackupInfoManager was not informed yet that the shipment
                    // was fully received). When node evacuate then tries to continue to delete the
                    // source-snapshot, we run into a "please wait until shipment is finished" error.
                    try
                    {
                        // sorry for this. a better way would be to register the the "finished receiving" flux
                        // but in the node evacuate context that is a bit cumbersome to figure out since
                        // autoplacer might be involved or the shipment might even be queued.

                        // on the other hand, it should not really matter that much if a shipment that can
                        // easily take multiple minutes to finish waits 2 more seconds before it is getting
                        // deleted...
                        Thread.sleep(2_000);
                    }
                    catch (InterruptedException exc)
                    {
                        Thread.currentThread().interrupt();
                        exc.printStackTrace();
                    }
                    for (FluxSink<ApiCallRc> sink : sinkList)
                    {
                        consumerRef.accept(sink);
                    }
                    innerSink.complete();
                });
            }
            else
            {
                ret = Flux.empty();
            }
        }
        return ret;
    }

    public class QueueItem implements Comparable<QueueItem>
    {
        public final SnapshotDefinition snapDfn;
        // this is either an S3Remote or an L2LRemote, never a StltRemote
        public final AbsRemote s3orLinRemote;
        /* if prevSnapDfn is null, it means a full backup should be made */
        public @Nullable SnapshotDefinition prevSnapDfn;
        public final @Nullable String preferredNode;
        public final @Nullable BackupShippingSrcData l2lData;
        /*
         * This is needed to make it possible that the queueItems from getFollowUpSnaps can be started on any available
         * node. Without alreadyStartedOn, an already finished shipping could be started again on another node.
         */
        public @Nullable Node alreadyStartedOn;
        /**
         * Needed for BackupNodeFinder's canUseNode to distinguish if a node needs to already have this snapshot or not.
         */
        public boolean shipExistingSnap;

        // TODO: spotbugs thinks that "remoteRef must be non-null but is marked as nullable" (non-null due to
        // @NonNullByDefault, nullable probably due to being off by one index during analysis). Once this bug is
        // resolved, remove the @SuppressFBWarnings annotation
        // spotbugs-issue: https://github.com/spotbugs/spotbugs/issues/3068
        @SuppressFBWarnings("NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE")
        private QueueItem(
            SnapshotDefinition snapDfnRef,
            AbsRemote remoteRef,
            @Nullable SnapshotDefinition prevSnapDfnRef,
            @Nullable String preferredNodeRef,
            @Nullable BackupShippingSrcData l2lDataRef,
            boolean shipExistingSnapRef
        )
        {
            snapDfn = snapDfnRef;
            s3orLinRemote = remoteRef;
            prevSnapDfn = prevSnapDfnRef;
            preferredNode = preferredNodeRef;
            l2lData = l2lDataRef;
            shipExistingSnap = shipExistingSnapRef;
        }

        @Override
        public String toString()
        {
            return "QueueItem [snapDfn=" + snapDfn + ", s3orLinRemote=" + s3orLinRemote + ", prevSnapDfn=" +
                prevSnapDfn + ", preferredNode=" + preferredNode + ", l2lData=" + l2lData + ", alreadyStartedOn=" +
                alreadyStartedOn + ", shipExistingSnap=" + shipExistingSnap + "]";
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + s3orLinRemote.hashCode();
            result = prime * result + snapDfn.hashCode();
            return result;
        }

        @Override
        public boolean equals(Object obj)
        {
            boolean equals = false;
            if (obj instanceof QueueItem)
            {
                if (this == obj)
                {
                    equals = true;
                }
                else
                {
                    QueueItem other = (QueueItem) obj;
                    equals = Objects.equals(s3orLinRemote, other.s3orLinRemote) &&
                        Objects.equals(snapDfn, other.snapDfn);
                }
            }
            return equals;
        }

        @Override
        public int compareTo(QueueItem other)
        {
            int ret = 0;
            try
            {
                String myDateStr = snapDfn.getSnapDfnProps(sysCtx)
                    .getProp(
                        InternalApiConsts.KEY_BACKUP_START_TIMESTAMP,
                        BackupShippingUtils.BACKUP_SOURCE_PROPS_NAMESPC + "/" + s3orLinRemote.getName().displayValue
                    );
                String otherDateStr = other.snapDfn.getSnapDfnProps(sysCtx)
                    .getProp(
                        InternalApiConsts.KEY_BACKUP_START_TIMESTAMP,
                        BackupShippingUtils.BACKUP_SOURCE_PROPS_NAMESPC + "/" + other.s3orLinRemote.getName().displayValue
                    );
                /*
                 * To prevent an ImplementationError from possibly completely stopping whatever thread is currently
                 * trying to sort these QueueItems, it is not thrown but instead creates an ErrorReport
                 */
                if (myDateStr == null || myDateStr.isEmpty())
                {
                    if (otherDateStr == null || otherDateStr.isEmpty())
                    {
                        ret = 0;
                        errorReporter.reportError(
                            new ImplementationError(
                                "The snapDfns '" + snapDfn + "' and '" + other.snapDfn +
                                    "' do not have the property " +
                                    "BackupShipping/Source/<remoteName>/BackupStartTimestamp set. Remotes: " +
                                    s3orLinRemote.getName() + " and " + other.s3orLinRemote.getName()
                            )
                        );
                    }
                    else
                    {
                        ret = -1;
                        errorReporter.reportError(
                            new ImplementationError(
                                "The snapDfns '" + snapDfn +
                                    "' does not have the property BackupShipping/Source/" + s3orLinRemote.getName() +
                                    "/BackupStartTimestamp set."
                            )
                        );
                    }
                }
                else
                {
                    if (otherDateStr == null || otherDateStr.isEmpty())
                    {
                        ret = 1;
                        errorReporter.reportError(
                            new ImplementationError(
                                "The snapDfns '" + other.snapDfn +
                                    "' does not have the property BackupShipping/Source/" + other.s3orLinRemote.getName() +
                                    "/BackupStartTimestamp set."
                            )
                        );
                    }
                    else
                    {
                        ret = Long.compare(Long.parseLong(myDateStr), Long.parseLong(otherDateStr));
                    }
                }
            }
            catch (AccessDeniedException exc)
            {
                throw new ImplementationError(exc);
            }
            return ret;
        }

    }

    public static class AbortInfo
    {
        public final List<AbortS3Info> abortS3InfoList = new ArrayList<>();
        public final List<AbortL2LInfo> abortL2LInfoList = new ArrayList<>();

        public boolean isEmpty()
        {
            return abortS3InfoList.isEmpty() && abortL2LInfoList.isEmpty();
        }
    }

    public static class AbortS3Info
    {
        public final String backupName;
        public final String uploadId;
        public final String remoteName;

        AbortS3Info(String backupNameRef, String uploadIdRef, String remoteNameRef)
        {
            backupName = backupNameRef;
            uploadId = uploadIdRef;
            remoteName = remoteNameRef;
        }
    }

    public static class AbortL2LInfo
    {
        // no special data needed (for now?)
    }

    public static class ShipmentFailedException extends LinStorException
    {
        public ShipmentFailedException(String messageRef)
        {
            super(messageRef);
        }
    }

    public static class CleanupData
    {
        public final BackupShippingSrcData data;
        private @Nullable StltRemoteCleanupTask task;
        private int finishedCount = 0;

        private CleanupData(BackupShippingSrcData dataRef)
        {
            data = dataRef;
        }

        public @Nullable StltRemoteCleanupTask getTask()
        {
            return task;
        }
    }
}
