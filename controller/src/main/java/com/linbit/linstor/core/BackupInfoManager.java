package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotDefinition.Key;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@Singleton
public class BackupInfoManager
{
    private Map<ResourceDefinition, String> restoreMap;
    private Map<NodeName, Map<SnapshotDefinition.Key, AbortInfo>> abortMap;
    private Map<Snapshot, LinkedList<Snapshot>> backupsToUpload;

    @Inject
    public BackupInfoManager(TransactionObjectFactory transObjFactoryRef)
    {
        restoreMap = transObjFactoryRef.createTransactionPrimitiveMap(new HashMap<>(), null);
        abortMap = new HashMap<>();
        backupsToUpload = new HashMap<>();
    }

    public boolean restoreAddEntry(ResourceDefinition rscDfn, String metaName)
    {
        synchronized (restoreMap)
        {
            if (!restoreMap.containsKey(rscDfn))
            {
                restoreMap.put(rscDfn, metaName);
                return true;
            }
            else
            {
                return false;
            }
        }
    }

    public void restoreRemoveEntry(ResourceDefinition rscDfn)
    {
        synchronized (restoreMap)
        {
            restoreMap.remove(rscDfn);
        }
    }

    public boolean restoreContainsRscDfn(ResourceDefinition rscDfn)
    {
        synchronized (restoreMap)
        {
            return restoreMap.containsKey(rscDfn);
        }
    }

    public boolean restoreContainsMetaFile(String metaName)
    {
        synchronized (restoreMap)
        {
            return restoreMap.containsValue(metaName);
        }
    }

    public boolean restoreContainsMetaFile(String rscName, String snapName)
    {
        synchronized (restoreMap)
        {
            return restoreMap.containsValue(rscName + "_" + snapName + ".meta");
        }
    }

    public void abortAddS3Entry(
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
        synchronized (abortMap)
        {
            getAbortInfo(nodeName, rscName, snapName).abortS3InfoList.add(
                new AbortS3Info(backupName, uploadId, remoteName)
            );
        }
    }

    public void abortAddL2LEntry(NodeName nodeName, Key key)
    {
        // DO NOT just synchronize within getAbortInfo as the following .add(new Abort*Info(..)) should also be in the
        // same synchronized block as the initial get
        synchronized (abortMap)
        {
            getAbortInfo(nodeName, key).abortL2LInfoList.add(new AbortL2LInfo());
        }
    }

    private AbortInfo getAbortInfo(String nodeName, String rscName, String snapName)
    {
        try
        {
            return getAbortInfo(
                new NodeName(nodeName),
                new SnapshotDefinition.Key(new ResourceName(rscName), new SnapshotName(snapName))
            );
        }
        catch (InvalidNameException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private AbortInfo getAbortInfo(NodeName nodeName, SnapshotDefinition.Key snapDfnKey)
    {
        Map<SnapshotDefinition.Key, AbortInfo> map = abortMap.get(nodeName);
        if (map == null)
        {
            map = new HashMap<>();
            abortMap.put(nodeName, map);
        }

        AbortInfo abortInfo = map.get(snapDfnKey);
        if (abortInfo == null)
        {
            abortInfo = new AbortInfo();
            map.put(snapDfnKey, abortInfo);
        }
        return abortInfo;
    }

    public void abortDeleteEntries(String nodeName, String rscName, String snapName) throws InvalidNameException
    {
        synchronized (abortMap)
        {
            abortDeleteEntries(
                new NodeName(nodeName),
                new SnapshotDefinition.Key(new ResourceName(rscName), new SnapshotName(snapName))
            );
        }
    }

    public void abortDeleteEntries(NodeName nodeName, SnapshotDefinition.Key snapDfnKey)
    {
        synchronized (abortMap)
        {
            Map<SnapshotDefinition.Key, AbortInfo> map = abortMap.get(nodeName);
            if (map != null)
            {
                map.remove(snapDfnKey);
            }
        }
    }

    public Map<SnapshotDefinition.Key, AbortInfo> abortGetEntries(NodeName nodeName)
    {
        synchronized (abortMap)
        {
            return abortMap.get(nodeName);
        }
    }

    public boolean backupsToUploadAddEntry(Snapshot snap, LinkedList<Snapshot> backups)
    {
        synchronized (backupsToUpload)
        {
            if (backupsToUpload.containsKey(snap))
            {
                return false;
            }
            backupsToUpload.put(snap, backups);
            return true;
        }
    }

    public Snapshot getNextBackupToUpload(Snapshot snap)
    {
        synchronized (backupsToUpload)
        {
            LinkedList<Snapshot> linkedList = backupsToUpload.get(snap);
            Snapshot ret = null;
            if (linkedList != null)
            {
                ret = linkedList.pollFirst();
            }
            return ret;
        }
    }

    public void backupsToUploadRemoveEntry(Snapshot snap)
    {
        synchronized (backupsToUpload)
        {
            backupsToUpload.remove(snap);
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
}
