package com.linbit.linstor.core;

import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.NodeData;
import com.linbit.linstor.netcom.Peer;

import java.util.UUID;

public interface ControllerPeerConnector
{
    NodeData getLocalNode();

    Peer getControllerPeer();

    void setControllerPeer(Peer controllerPeerRef, UUID nodeUuid, String nodeName);

    void setControllerPeerToCurrentLocalNode();

    NodeName getLocalNodeName();
}
