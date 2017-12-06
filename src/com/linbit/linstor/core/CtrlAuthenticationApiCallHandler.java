package com.linbit.linstor.core;

import java.io.IOException;

import com.linbit.ImplementationError;
import com.linbit.linstor.api.interfaces.serializer.CtrlAuthSerializer;
import com.linbit.linstor.netcom.IllegalMessageStateException;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;

class CtrlAuthenticationApiCallHandler
{
    private Controller controller;
    private CtrlAuthSerializer serializer;

    public CtrlAuthenticationApiCallHandler(
        Controller controllerRef,
        CtrlAuthSerializer serializerRef
    )
    {
        controller = controllerRef;
        serializer = serializerRef;
    }

    public void completeAuthentication(Peer peer)
    {
        try
        {
            controller.getErrorReporter().logDebug("Sending authentication to satellite '" + peer.getNode().getName() + "'");
            Message msg = peer.createMessage();
            // TODO make the shared secret customizable
            msg.setData(serializer.getAuthMessage(peer.getNode(), "Hello, LinStor!".getBytes()));
            peer.sendMessage(msg);
        }
        catch (IllegalMessageStateException illegalMessageStateExc)
        {
            controller.getErrorReporter().reportError(
                new ImplementationError(
                    "Failed to complete authentication to satellite.",
                    illegalMessageStateExc
                )
            );
        }
        catch (IOException e)
        {
            controller.getErrorReporter().reportError(
                e,
                null,
                peer,
                "Could not complete authentication due to an IOException"
            );
        }
    }
}
