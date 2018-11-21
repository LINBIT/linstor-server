package com.linbit.linstor.storage.layer.adapter.drbd;

import com.linbit.linstor.NodeId;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.storage2.layer.data.DrbdRscData;

import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

public class DrbdRscDataStlt implements DrbdRscData
{
    NodeId nodeId;
    boolean disklessForPeers;
    boolean diskless;
    boolean failed;

    transient boolean exists = false;
    transient boolean requiresAdjust = false;
    transient boolean isPrimary;
    transient boolean isSuspended = false;

    final transient Map<VolumeNumber, DrbdVlmDataStlt> vlmStates;

    public DrbdRscDataStlt(
        NodeId nodeIdRef,
        boolean disklessForPeersRef,
        boolean disklessRef
    )
    {
        nodeId = nodeIdRef;
        disklessForPeers = disklessForPeersRef;
        diskless = disklessRef;

        vlmStates = new TreeMap<>();
    }

    @Override
    public NodeId getNodeId()
    {
        return nodeId;
    }

    @Override
    public boolean disklessForPeers()
    {
        return disklessForPeers;
    }

    @Override
    public boolean isDiskless()
    {
        return diskless;
    }

    @Override
    public boolean isFailed()
    {
        return failed;
    }

    void putVlmState(VolumeNumber vlmNr, DrbdVlmDataStlt vlmState)
    {
        vlmStates.put(vlmNr, vlmState);
    }

    Stream<DrbdVlmDataStlt> streamVolumeStates()
    {
        return vlmStates.values().stream();
    }
}
