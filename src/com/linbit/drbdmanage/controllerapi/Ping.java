package com.linbit.drbdmanage.controllerapi;

import com.linbit.ImplementationError;
import com.linbit.drbdmanage.Controller;
import com.linbit.drbdmanage.CoreServices;
import com.linbit.drbdmanage.netcom.IllegalMessageStateException;
import com.linbit.drbdmanage.netcom.Message;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.netcom.TcpConnector;
import com.linbit.drbdmanage.proto.MsgHeaderOuterClass.MsgHeader;
import com.linbit.drbdmanage.security.AccessContext;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Simple Ping request
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class Ping implements com.linbit.drbdmanage.ApiCall
{
    private Controller      ctrl;
    private CoreServices    coreSvcs;

    @Override
    public String getName()
    {
        return Ping.class.getSimpleName();
    }

    public Ping(
        Controller      ctrlRef,
        CoreServices    coreSvcsRef
    )
    {
        ctrl = ctrlRef;
        coreSvcs = coreSvcsRef;
    }

    @Override
    public void execute(
        AccessContext   accCtx,
        Message         msg,
        int             msgId,
        InputStream     msgDataIn,
        TcpConnector    connector,
        Peer            client
    )
    {
        try
        {
            MsgHeader.Builder headerBuilder = MsgHeader.newBuilder();
            headerBuilder.setMsgId(msgId);
            headerBuilder.setApiCall("Pong");
            MsgHeader header = headerBuilder.build();

            Message pongMsg = client.createMessage();
            ByteArrayOutputStream dataOut = new ByteArrayOutputStream();
            header.writeDelimitedTo(dataOut);
            pongMsg.setData(dataOut.toByteArray());

            client.sendMessage(pongMsg);
        }
        catch (IOException ioExc)
        {
            coreSvcs.getErrorReporter().reportError(ioExc);
        }
        catch (IllegalMessageStateException msgExc)
        {
            throw new ImplementationError(
                Message.class.getName() + " object returned by the " + Peer.class.getName() +
                " class has an illegal state",
                msgExc
            );
        }
    }
}
