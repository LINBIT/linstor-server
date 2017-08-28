package com.linbit.drbdmanage.api.common;

import com.linbit.ImplementationError;
import com.linbit.drbdmanage.CoreServices;
import com.linbit.drbdmanage.DrbdManage;
import com.linbit.drbdmanage.api.BaseApiCall;
import com.linbit.drbdmanage.netcom.IllegalMessageStateException;
import com.linbit.drbdmanage.netcom.Message;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.netcom.TcpConnector;
import com.linbit.drbdmanage.security.AccessContext;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Simple Ping request
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class Ping extends BaseApiCall
{
    private DrbdManage      ctrl;
    private CoreServices    coreSvcs;

    @Override
    public String getName()
    {
        return Ping.class.getSimpleName();
    }

    public Ping(
        DrbdManage      ctrlRef,
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
            Message pongMsg = client.createMessage();
            ByteArrayOutputStream dataOut = new ByteArrayOutputStream();
            writeMsgHeader(dataOut, msgId, "Pong");
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
