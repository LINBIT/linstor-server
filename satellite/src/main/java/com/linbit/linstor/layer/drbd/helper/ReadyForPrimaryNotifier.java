package com.linbit.linstor.layer.drbd.helper;

import com.linbit.linstor.drbdstate.DrbdConnection;
import com.linbit.linstor.drbdstate.DrbdResource;
import com.linbit.linstor.drbdstate.DrbdVolume;
import com.linbit.linstor.drbdstate.ResourceObserver;
import com.linbit.linstor.drbdstate.DrbdVolume.DiskState;

public class ReadyForPrimaryNotifier implements ResourceObserver
{
    private final String rscName;
    private final Object syncObj;

    public ReadyForPrimaryNotifier(String rscNameRef, Object syncObjRef)
    {
        rscName = rscNameRef;
        syncObj = syncObjRef;
    }

    @Override
    public void diskStateChanged(
        DrbdResource drbdResource,
        DrbdConnection connection,
        DrbdVolume volume,
        DiskState previous,
        DiskState current
    )
    {
        if (drbdResource.getNameString().equals(rscName) &&
            hasValidStateForPrimary(drbdResource))
        {
            synchronized (syncObj)
            {
                syncObj.notify();
            }
        }
    }

    public boolean hasValidStateForPrimary(DrbdResource drbdResource)
    {
        return
            drbdResource.getVolumesMap().values().stream().allMatch(
                drbdVlm ->
                    drbdVlm.getDiskState().oneOf(
                        DiskState.CONSISTENT,
                        DiskState.DISKLESS,
                        DiskState.FAILED,
                        DiskState.INCONSISTENT,
                        DiskState.OUTDATED,
                        DiskState.UP_TO_DATE
                    )
        );
    }

}
