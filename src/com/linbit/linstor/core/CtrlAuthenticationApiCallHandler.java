package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.Node;
import com.linbit.linstor.api.interfaces.serializer.InterComSerializer;
import com.linbit.linstor.netcom.IllegalMessageStateException;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

class CtrlAuthenticationApiCallHandler
{
    private ApiCtrlAccessors apiCtrlAccessors;
    private InterComSerializer serializer;
    private AccessContext apiCtx;

    public CtrlAuthenticationApiCallHandler(
        ApiCtrlAccessors apiCtrlAccessorsRef,
        InterComSerializer serializerRef,
        AccessContext apiCtxRef
    )
    {
        apiCtrlAccessors = apiCtrlAccessorsRef;
        serializer = serializerRef;
        apiCtx = apiCtxRef;
    }

    public void completeAuthentication(Peer peer)
    {
        try
        {
            Node peerNode = peer.getNode();
            apiCtrlAccessors.getErrorReporter().logDebug("Sending authentication to satellite '" + peerNode.getName() + "'");
            Message msg = peer.createMessage();
            // TODO make the shared secret customizable
            msg.setData(serializer
                    .builder(InternalApiConsts.API_AUTH, 1)
                    .authMessage(
                        peerNode.getUuid(),
                        peerNode.getName().getDisplayName(),
                        "Hello, LinStor!".getBytes(),
                        peerNode.getDisklessStorPool(apiCtx).getUuid()
                    )
                    .build()
            );
            peer.sendMessage(msg);
        }
        catch (IllegalMessageStateException illegalMessageStateExc)
        {
            apiCtrlAccessors.getErrorReporter().reportError(
                new ImplementationError(
                    "Failed to complete authentication to satellite.",
                    illegalMessageStateExc
                )
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            apiCtrlAccessors.getErrorReporter().reportError(
                new ImplementationError(
                    "Could not serialize node's content for authentication.",
                    accDeniedExc
                )
            );
        }
    }
}
