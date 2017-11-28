package com.linbit.linstor.core;

import java.io.IOException;

import com.linbit.ImplementationError;
import com.linbit.linstor.api.interfaces.AuthSerializer;
import com.linbit.linstor.netcom.IllegalMessageStateException;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;

public class CtrlAuthenticationApiCallHandler
{
    private Controller controller;
    private AuthSerializer serializer;

    public CtrlAuthenticationApiCallHandler(
        Controller controllerRef,
        AuthSerializer serializerRef)
    {
        controller = controllerRef;
        serializer = serializerRef;
    }

    public void completeAuthentication(Peer peer)
    {
        try
        {
            Message msg = peer.createMessage();
            // TODO make the shared secret customizable
            msg.setData(serializer.getAuthMessage("Hello LinStor!".getBytes()));
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
