package com.linbit.linstor.core;

import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.HashMap;
import java.util.Map;

@Singleton
public class BackupInfoManager
{
    private Map<ResourceDefinition, String> restoreMap;

    @Inject
    public BackupInfoManager(TransactionObjectFactory transObjFactoryRef)
    {
        restoreMap = transObjFactoryRef.createTransactionPrimitiveMap(new HashMap<>(), null);
    }

    public boolean addEntry(ResourceDefinition rscDfn, String metaName)
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

    public void removeEntry(ResourceDefinition rscDfn)
    {
        synchronized (restoreMap)
        {
            restoreMap.remove(rscDfn);
        }
    }

    public boolean containsRscDfn(ResourceDefinition rscDfn)
    {
        synchronized (restoreMap)
        {
            return restoreMap.containsKey(rscDfn);
        }
    }

    public boolean containsMetaFile(String metaName)
    {
        synchronized (restoreMap)
        {
            return restoreMap.containsValue(metaName);
        }
    }

    public boolean containsMetaFile(String rscName, String snapName)
    {
        synchronized (restoreMap)
        {
            return restoreMap.containsValue(rscName + "_" + snapName + ".meta");
        }
    }

}
