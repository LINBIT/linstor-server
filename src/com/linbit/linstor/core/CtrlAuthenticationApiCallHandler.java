package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.interfaces.serializer.InterComSerializer;
import com.linbit.linstor.netcom.IllegalMessageStateException;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;

import java.io.IOException;

class CtrlAuthenticationApiCallHandler
{
    private ApiCtrlAccessors apiCtrlAccessors;
    private InterComSerializer serializer;

    public CtrlAuthenticationApiCallHandler(
        ApiCtrlAccessors apiCtrlAccessorsRef,
        InterComSerializer serializerRef
    )
    {
        apiCtrlAccessors = apiCtrlAccessorsRef;
        serializer = serializerRef;
    }

    public void completeAuthentication(Peer peer)
    {
        try
        {
            apiCtrlAccessors.getErrorReporter().logDebug("Sending authentication to satellite '" + peer.getNode().getName() + "'");
            Message msg = peer.createMessage();
            // TODO make the shared secret customizable
            msg.setData(serializer
                    .builder(InternalApiConsts.API_AUTH, 1)
                    .authMessage(
                            peer.getNode().getUuid(),
                            peer.getNode().getName().getDisplayName(),
                            "Hello, LinStor!".getBytes())
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
    }
}
