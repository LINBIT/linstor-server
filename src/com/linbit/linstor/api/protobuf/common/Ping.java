package com.linbit.linstor.api.protobuf.common;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import com.linbit.ImplementationError;
import com.linbit.linstor.CoreServices;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.netcom.IllegalMessageStateException;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;

/**
 * Simple Ping request
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
@ProtobufApiCall
public class Ping extends BaseProtoApiCall
{
    private LinStor      ctrl;
    private CoreServices coreSvcs;

    @Override
    public String getName()
    {
        return Ping.class.getSimpleName();
    }

    @Override
    public String getDescription()
    {
        return "Ping: Communication test. Responds with a Pong message.";
    }

    public Ping(
        LinStor      ctrlRef,
        CoreServices    coreSvcsRef
    )
    {
        super(coreSvcsRef.getErrorReporter());
        ctrl = ctrlRef;
        coreSvcs = coreSvcsRef;
    }

    @Override
    public void executeImpl(
        AccessContext   accCtx,
        Message         msg,
        int             msgId,
        InputStream     msgDataIn,
        Peer            client
    )
        throws IOException
    {
        try
        {
            Message pongMsg = client.createMessage();
            ByteArrayOutputStream dataOut = new ByteArrayOutputStream();
            writeProtoMsgHeader(dataOut, msgId, "Pong");
            pongMsg.setData(dataOut.toByteArray());

            client.sendMessage(pongMsg);
        }
        catch (IllegalMessageStateException msgExc)
        {
            coreSvcs.getErrorReporter().reportError(
                new ImplementationError(
                    Message.class.getName() + " object returned by the " + Peer.class.getName() +
                    " class has an illegal state",
                    msgExc
                )
            );
        }
    }
}
