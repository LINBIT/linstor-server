package com.linbit.linstor.core;

import com.linbit.linstor.Resource;
import com.linbit.linstor.Snapshot;

import java.util.Collection;

public interface DeviceHandler
{
    void dispatchResource(Resource rsc, Collection<Snapshot> inProgressSnapshots);
}
