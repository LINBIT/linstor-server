package com.linbit.linstor.core;

import java.io.IOException;

import com.linbit.ImplementationError;
import com.linbit.linstor.api.interfaces.serializer.CtrlAuthSerializer;
import com.linbit.linstor.netcom.IllegalMessageStateException;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;

class CtrlAuthenticationApiCallHandler
{
    private ApiCtrlAccessors apiCtrlAccessors;
    private CtrlAuthSerializer serializer;

    public CtrlAuthenticationApiCallHandler(
        ApiCtrlAccessors apiCtrlAccessorsRef,
        CtrlAuthSerializer serializerRef
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
            msg.setData(serializer.getAuthMessage(peer.getNode(), "Hello, LinStor!".getBytes()));
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
        catch (IOException e)
        {
            apiCtrlAccessors.getErrorReporter().reportError(
                e,
                null,
                peer,
                "Could not complete authentication due to an IOException"
            );
        }
    }
}
