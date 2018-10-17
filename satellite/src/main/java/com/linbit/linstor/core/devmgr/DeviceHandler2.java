package com.linbit.linstor.core.devmgr;

import com.linbit.linstor.Resource;
import com.linbit.linstor.Snapshot;

import java.util.Collection;

public interface DeviceHandler2
{
    void dispatchResource(
        Collection<Resource> rscs,
        Collection<Snapshot> snapshots
    );
}
