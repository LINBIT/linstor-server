package com.linbit.linstor.storage.interfaces.layers.drbd;

import com.linbit.linstor.NodeId;
import com.linbit.linstor.storage.interfaces.categories.RscLayerObject;

public interface DrbdRscObject extends RscLayerObject
{
    NodeId getNodeId();

    boolean isDiskless();

    boolean isDisklessForPeers();
}
