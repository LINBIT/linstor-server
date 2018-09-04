package com.linbit.linstor.core;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.Node;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import javax.inject.Inject;

public class CtrlAuthenticator
{
    private final ErrorReporter errorReporter;
    private final CtrlStltSerializer serializer;

    @Inject
    CtrlAuthenticator(
        ErrorReporter errorReporterRef,
        CtrlStltSerializer serializerRef
    )
    {
        errorReporter = errorReporterRef;
        serializer = serializerRef;
    }

    public void completeAuthentication(Peer peer)
    {
        Node peerNode = peer.getNode();
        if (peerNode.isDeleted())
        {
            errorReporter.logWarning(
                "Unable to complete authentication with peer '%s' because the node has been deleted", peer);
        }
        else
        {
            errorReporter.logDebug("Sending authentication to satellite '" +
                peerNode.getName() + "'");
            // TODO make the shared secret customizable
            peer.sendMessage(
                serializer
                    .onewayBuilder(InternalApiConsts.API_AUTH)
                    .authMessage(
                        peerNode.getUuid(),
                        peerNode.getName().getDisplayName(),
                        "Hello, LinStor!".getBytes()
                    )
                    .build()
            );
        }
    }
}
