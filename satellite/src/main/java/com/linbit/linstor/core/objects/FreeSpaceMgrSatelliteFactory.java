package com.linbit.linstor.core.objects;

import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;

import java.util.Map;
import java.util.TreeMap;

public class FreeSpaceMgrSatelliteFactory
{
    private final Map<SharedStorPoolName, StltFreeSpaceMgr> stltFreeSpaceMgrMap;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public FreeSpaceMgrSatelliteFactory(
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        transMgrProvider = transMgrProviderRef;
        stltFreeSpaceMgrMap = new TreeMap<>();
    }

    public FreeSpaceTracker getInstance(SharedStorPoolName sharedStorPoolName)
    {
        StltFreeSpaceMgr freeSpace;
        synchronized (stltFreeSpaceMgrMap)
        {
            freeSpace = stltFreeSpaceMgrMap.get(sharedStorPoolName);
            if (freeSpace == null)
            {
                freeSpace = new StltFreeSpaceMgr(transMgrProvider, sharedStorPoolName);
                stltFreeSpaceMgrMap.put(sharedStorPoolName, freeSpace);
            }
            return freeSpace;
        }
    }
}
