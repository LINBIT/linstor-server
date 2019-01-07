package com.linbit.linstor.storage.layer.kinds;

import com.linbit.linstor.Node;
import com.linbit.linstor.ResourceDefinition;

import java.util.Set;

public class DrbdProxyLayerKind implements DeviceLayerKind
{
    @Override
    public boolean isSnapshotSupported()
    {
        return false;
    }

    @Override
    public boolean isResizeSupported()
    {
        return true;
    }

    @Override
    public StartupVerifications[] requiredVerifications()
    {
        return new StartupVerifications[]
        {
            StartupVerifications.DRBD_PROXY
        };
    }

    @Override
    public Set<Node> getSpecialInterestedNodes(ResourceDefinition rscDfn)
    {
        // TODO: implement
        return null;
    }
}
