package com.linbit.linstor.core;

import com.linbit.linstor.NodeName;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.Snapshot;

import java.util.Collection;

public interface DeviceHandler
{
    void dispatchResource(
        ResourceDefinition rscDfn,
        Collection<Snapshot> inProgressSnapshots
    );
}
