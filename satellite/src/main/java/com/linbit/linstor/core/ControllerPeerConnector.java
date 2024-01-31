package com.linbit.linstor.core;

import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.netcom.Peer;

import javax.annotation.Nullable;

import java.util.UUID;

public interface ControllerPeerConnector
{
    Node getLocalNode();

    Peer getControllerPeer();

    /**
     * Sets the controller peer object used later for replies to the controller.
     *
     * @param ctrlUuidRef
     * @param controllerPeerRef
     * @param nodeUuid
     * @param nodeName
     */
    void setControllerPeer(
        @Nullable UUID ctrlUuidRef,
        Peer controllerPeerRef,
        UUID nodeUuid,
        String nodeName
    );

    void setControllerPeerToCurrentLocalNode();

    NodeName getLocalNodeName();
}
