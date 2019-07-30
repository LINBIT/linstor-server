package com.linbit.linstor.core.objects;

import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;

public class FreeSpaceMgrSatelliteFactory
{
    private final StltFreeSpaceMgr stltFreeSpaceMgr;

    @Inject
    public FreeSpaceMgrSatelliteFactory(
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        stltFreeSpaceMgr = new StltFreeSpaceMgr(transMgrProviderRef);
    }

    public FreeSpaceTracker getInstance()
    {
        return stltFreeSpaceMgr;
    }
}
