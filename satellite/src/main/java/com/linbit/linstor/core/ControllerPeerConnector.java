package com.linbit.linstor.core;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.netcom.Peer;

import java.util.UUID;

public interface ControllerPeerConnector
{
    @Nullable
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

    @Nullable
    NodeName getLocalNodeName();
}
