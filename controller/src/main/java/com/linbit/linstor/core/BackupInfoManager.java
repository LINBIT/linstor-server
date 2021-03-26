package com.linbit.linstor.core;

import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Singleton
public class BackupInfoManager
{
    private Map<ResourceDefinition, String> restoreMap;
    private Map<String, Map<Pair<String, String>, List<AbortInfo>>> abortMap;

    @Inject
    public BackupInfoManager(TransactionObjectFactory transObjFactoryRef)
    {
        restoreMap = transObjFactoryRef.createTransactionPrimitiveMap(new HashMap<>(), null);
        abortMap = new HashMap<>();
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
            String val = restoreMap.remove(rscDfn);
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

    public void abortAddEntry(String nodeName, String rscName, String snapName, String backupName, String uploadId)
    {
        Pair<String, String> pair = new Pair<>(rscName, snapName);

        Map<Pair<String, String>, List<AbortInfo>> map = abortMap.get(nodeName);
        if (map != null)
        {
            if (map.containsKey(pair))
            {
                map.get(pair).add(new AbortInfo(backupName, uploadId));
            }
            else
            {
                map.put(pair, new ArrayList<>());
                map.get(pair).add(new AbortInfo(backupName, uploadId));
            }
        }
        else
        {
            abortMap.put(nodeName, new HashMap<>());
            map = abortMap.get(nodeName);
            map.put(pair, new ArrayList<>());
            map.get(pair).add(new AbortInfo(backupName, uploadId));
        }
    }

    public void abortDeleteEntries(String nodeName, String rscName, String snapName)
    {
        Pair<String, String> pair = new Pair<>(rscName, snapName);

        Map<Pair<String, String>, List<AbortInfo>> map = abortMap.get(nodeName);
        if (map != null)
        {
            map.remove(pair);
        }
    }

    public Map<Pair<String, String>, List<AbortInfo>> abortGetEntries(String nodeName)
    {
        return abortMap.get(nodeName);
    }

    public class AbortInfo
    {
        public final String backupName;
        public final String uploadId;

        AbortInfo(String backupNameRef, String uploadIdRef)
        {
            backupName = backupNameRef;
            uploadId = uploadIdRef;
        }
    }

}
