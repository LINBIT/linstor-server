package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.core.apicallhandler.controller.backup.CtrlBackupL2LDstApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.backup.CtrlBackupL2LSrcApiCallHandler;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Singleton
public class BackupInfoManager
{
    private final Map<ResourceDefinition, String> restoreMap;
    private final Map<NodeName, Map<SnapshotDefinition.Key, AbortInfo>> abortCreateMap;
    private final Map<ResourceName, Set<Snapshot>> abortRestoreMap;
    private final Map<Snapshot, Snapshot> backupsToDownload;
    private final Map<RemoteName, CtrlBackupL2LSrcApiCallHandler.BackupShippingData> l2lSrcData;
    private final Map<Snapshot, CtrlBackupL2LDstApiCallHandler.BackupShippingData> l2lDstData;

    @Inject
    public BackupInfoManager(TransactionObjectFactory transObjFactoryRef)
    {
        restoreMap = transObjFactoryRef.createTransactionPrimitiveMap(new HashMap<>(), null);
        abortCreateMap = new HashMap<>();
        abortRestoreMap = new HashMap<>();
        backupsToDownload = new HashMap<>();
        l2lSrcData = new HashMap<>();
        l2lDstData = new HashMap<>();
    }

    /**
     * mark a rscDfn as target of a backup restore. rscDfns in this map should not be modified in any way
     * also add the backup that is the source of the restore, to avoid multiple restores
     * of the same backup at the same time
     */
    public boolean restoreAddEntry(ResourceDefinition rscDfn, String metaName)
    {
        boolean addFlag;
        synchronized (restoreMap)
        {
            addFlag = !restoreMap.containsKey(rscDfn);
            if (addFlag)
            {
                restoreMap.put(rscDfn, metaName);
            }
        }
        return addFlag;
    }

    /**
     * unmark the rscDfn to signify the backup restore is done and allow other modifications to take place
     * and also free the source backup for the next restore
     */
    public void restoreRemoveEntry(ResourceDefinition rscDfn)
    {
        synchronized (restoreMap)
        {
            restoreMap.remove(rscDfn);
        }
    }

    /**
     * check if a rscDfn has been marked as a target of a restore
     */
    public boolean restoreContainsRscDfn(ResourceDefinition rscDfn)
    {
        synchronized (restoreMap)
        {
            return restoreMap.containsKey(rscDfn);
        }
    }

    /**
     * check if a certain backup is currently being restored
     */
    public boolean restoreContainsMetaFile(String metaName)
    {
        synchronized (restoreMap)
        {
            return restoreMap.containsValue(metaName);
        }
    }

    /**
     * abortRestore saves a list of snapshots used in a restore for each rscDfn that need to be
     * taken care of in case of an abort. This method adds a snapshot to that list
     */
    public void abortRestoreAddEntry(String rscNameStr, Snapshot snap)
    {
        try
        {
            synchronized (abortRestoreMap)
            {
                ResourceName rscName = new ResourceName(rscNameStr);
                Set<Snapshot> snaps = abortRestoreMap.computeIfAbsent(rscName, k -> new HashSet<>());
                snaps.add(snap);
            }
        }
        catch (InvalidNameException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    /**
     * get all the restore-related snapshots that need to be aborted of a specific rscDfn
     */
    public Set<Snapshot> abortRestoreGetEntries(String rscNameStr)
    {
        try
        {
            synchronized (abortRestoreMap)
            {
                ResourceName rscName = new ResourceName(rscNameStr);
                return abortRestoreMap.get(rscName);
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
    public void abortRestoreDeleteAllEntries(String rscNameStr)
    {
        try
        {
            synchronized (abortRestoreMap)
            {
                ResourceName rscName = new ResourceName(rscNameStr);
                abortRestoreMap.remove(rscName);
            }
        }
        catch (InvalidNameException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    /**
     * remove a single snapshot from the list associated with the given rscDfn,
     * signifying that this snapshot is completed and no longer needs aborting
     */
    public void abortRestoreDeleteEntry(String rscNameStr, Snapshot snap)
    {
        try
        {
            synchronized (abortRestoreMap)
            {
                ResourceName rscName = new ResourceName(rscNameStr);
                Set<Snapshot> snaps = abortRestoreMap.get(rscName);
                if (snaps != null)
                {
                    snaps.remove(snap);
                }
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
        String nodeName,
        String rscName,
        String snapName,
        String backupName,
        String uploadId,
        String remoteName
    )
    {
        // DO NOT just synchronize within getAbortInfo as the following .add(new Abort*Info(..)) should also be in the
        // same synchronized block as the initial get
        synchronized (abortCreateMap)
        {
            getAbortCreateInfo(nodeName, rscName, snapName).abortS3InfoList.add(
                new AbortS3Info(backupName, uploadId, remoteName)
            );
        }
    }

    /**
     * add information about a l2l-shipping to a list to automatically abort it when issues like loss of connection
     * arise
     */
    public void abortCreateAddL2LEntry(NodeName nodeName, SnapshotDefinition.Key key)
    {
        // DO NOT just synchronize within getAbortInfo as the following .add(new Abort*Info(..)) should also be in the
        // same synchronized block as the initial get
        synchronized (abortCreateMap)
        {
            getAbortCreateInfo(nodeName, key).abortL2LInfoList.add(new AbortL2LInfo());
        }
    }

    private AbortInfo getAbortCreateInfo(String nodeName, String rscName, String snapName)
    {
        try
        {
            return getAbortCreateInfo(
                new NodeName(nodeName),
                new SnapshotDefinition.Key(new ResourceName(rscName), new SnapshotName(snapName))
            );
        }
        catch (InvalidNameException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private AbortInfo getAbortCreateInfo(NodeName nodeName, SnapshotDefinition.Key snapDfnKey)
    {
        Map<SnapshotDefinition.Key, AbortInfo> map = abortCreateMap.computeIfAbsent(nodeName, k -> new HashMap<>());
        return map.computeIfAbsent(snapDfnKey, a -> new AbortInfo());
    }

    /**
     * delete the abort-information given when it is no longer needed
     */
    public void abortCreateDeleteEntries(String nodeName, String rscName, String snapName) throws InvalidNameException
    {
        synchronized (abortCreateMap)
        {
            abortCreateDeleteEntries(
                new NodeName(nodeName),
                new SnapshotDefinition.Key(new ResourceName(rscName), new SnapshotName(snapName))
            );
        }
    }

    /**
     * delete the abort-information given when it is no longer needed
     */
    public void abortCreateDeleteEntries(NodeName nodeName, SnapshotDefinition.Key snapDfnKey)
    {
        synchronized (abortCreateMap)
        {
            Map<SnapshotDefinition.Key, AbortInfo> map = abortCreateMap.get(nodeName);
            if (map != null)
            {
                map.remove(snapDfnKey);
            }
        }
    }

    /**
     * get the abort-information to use it
     */
    public Map<SnapshotDefinition.Key, AbortInfo> abortCreateGetEntries(NodeName nodeName)
    {
        synchronized (abortCreateMap)
        {
            return abortCreateMap.get(nodeName);
        }
    }

    /**
     * add a pair of snapshots, with the second snapshot being the first snapshot's successor
     * these are used to determine which backups need to be downloaded during a restore
     */
    public boolean backupsToDownloadAddEntry(Snapshot snap, Snapshot successor)
    {
        boolean addFlag;
        synchronized (backupsToDownload)
        {
            addFlag = !backupsToDownload.containsKey(snap);
            if (addFlag)
            {
                backupsToDownload.put(snap, successor);
            }
        }
        return addFlag;
    }

    /**
     * get the successor of the given snapshot and delete the entry
     */
    public Snapshot getNextBackupToDownload(Snapshot snap)
    {
        synchronized (backupsToDownload)
        {
            return backupsToDownload.remove(snap);
        }
    }

    /**
     * clean up the download-map when the restore is aborted by anything
     */
    public void backupsToDownloadCleanUp(Snapshot snap)
    {
        synchronized (backupsToDownload)
        {
            Snapshot toDelete = backupsToDownload.remove(snap);
            while (toDelete != null)
            {
                toDelete = backupsToDownload.remove(toDelete);
            }
        }
    }

    public void addL2LSrcData(RemoteName name, CtrlBackupL2LSrcApiCallHandler.BackupShippingData data)
    {
        l2lSrcData.put(name, data);
    }

    public CtrlBackupL2LSrcApiCallHandler.BackupShippingData getL2LSrcData(RemoteName name)
    {
        return l2lSrcData.get(name);
    }

    public void removeL2LSrcData(RemoteName name)
    {
        l2lSrcData.remove(name);
    }

    public void addL2LDstData(Snapshot snap, CtrlBackupL2LDstApiCallHandler.BackupShippingData data)
    {
        l2lDstData.put(snap, data);
    }

    public CtrlBackupL2LDstApiCallHandler.BackupShippingData getL2LDstData(Snapshot snap)
    {
        return l2lDstData.get(snap);
    }

    public void removeL2LDstData(Snapshot snap)
    {
        l2lDstData.remove(snap);
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
}
