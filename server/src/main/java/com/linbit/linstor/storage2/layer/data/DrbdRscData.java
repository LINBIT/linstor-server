package com.linbit.linstor.storage2.layer.data;

import com.linbit.linstor.NodeId;
import com.linbit.linstor.storage2.layer.data.categories.RscLayerData;

public interface DrbdRscData extends RscLayerData
{
    NodeId getNodeId();

    boolean disklessForPeers();

    boolean isDiskless();
}
