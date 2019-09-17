package com.linbit.linstor.core;

import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.netcom.Peer;

import java.util.UUID;

public interface ControllerPeerConnector
{
    Node getLocalNode();

    Peer getControllerPeer();

    void setControllerPeer(Peer controllerPeerRef, UUID nodeUuid, String nodeName);

    void setControllerPeerToCurrentLocalNode();

    NodeName getLocalNodeName();
}
