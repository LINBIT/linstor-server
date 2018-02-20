package com.linbit.linstor.core;

import com.linbit.linstor.NodeData;
import com.linbit.linstor.StorPoolDefinitionData;
import com.linbit.linstor.netcom.Peer;

import java.util.UUID;

public interface ControllerPeerConnector
{
    NodeData getLocalNode();

    Peer getControllerPeer();

    StorPoolDefinitionData getDisklessStorPoolDfn();

    void setControllerPeer(
        Peer controllerPeerRef,
        UUID nodeUuid,
        String nodeName,
        UUID disklessStorPoolDfnUuid,
        UUID disklessStorPoolUuid
    );

    void setControllerPeerToCurrentLocalNode();
}
