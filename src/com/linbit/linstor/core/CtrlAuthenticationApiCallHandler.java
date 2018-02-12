package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.Node;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class CtrlAuthenticationApiCallHandler
{
    private final ErrorReporter errorReporter;
    private final CtrlStltSerializer serializer;
    private final AccessContext apiCtx;

    @Inject
    CtrlAuthenticationApiCallHandler(
        ErrorReporter errorReporterRef,
        CtrlStltSerializer serializerRef,
        AccessContext apiCtxRef
    )
    {
        errorReporter = errorReporterRef;
        serializer = serializerRef;
        apiCtx = apiCtxRef;
    }

    public void completeAuthentication(Peer peer)
    {
        try
        {
            Node peerNode = peer.getNode();
            errorReporter.logDebug("Sending authentication to satellite '" +
                peerNode.getName() + "'");
            // TODO make the shared secret customizable
            peer.sendMessage(
                serializer
                    .builder(InternalApiConsts.API_AUTH, 1)
                    .authMessage(
                        peerNode.getUuid(),
                        peerNode.getName().getDisplayName(),
                        "Hello, LinStor!".getBytes(),
                        peerNode.getDisklessStorPool(apiCtx).getDefinition(apiCtx).getUuid(),
                        peerNode.getDisklessStorPool(apiCtx).getUuid()
                    )
                    .build()
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            errorReporter.reportError(
                new ImplementationError(
                    "Could not serialize node's content for authentication.",
                    accDeniedExc
                )
            );
        }
    }
}
