package com.linbit.drbdmanage.debug;

import com.linbit.drbdmanage.CoreServices;
import com.linbit.drbdmanage.netcom.IllegalMessageStateException;
import com.linbit.drbdmanage.netcom.Message;
import com.linbit.drbdmanage.netcom.MessageProcessor;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.netcom.TcpConnector;
import com.linbit.drbdmanage.security.AccessContext;

/**
 * Debugger for messages
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class DebugMessageProcessor implements MessageProcessor
{
    CoreServices coreSvcs;

    public DebugMessageProcessor(CoreServices coreSvcsRef)
    {
        coreSvcs = coreSvcsRef;
    }

    @Override
    public void processMessage(Message msg, TcpConnector connector, Peer client)
    {
        try
        {
            System.out.println("MessageProcessor.processMessage() called");
            AccessContext accCtx = client.getAccessContext();
            System.out.printf(
                "Message from '%s' (Role '%s', Domain '%s') received from peer\n",
                accCtx.getIdentity(), accCtx.getRole(), accCtx.getDomain()
            );
            HexViewer.dumpByteArray(msg.getData());
        }
        catch (IllegalMessageStateException msgExc)
        {
            coreSvcs.getErrorReporter().reportError(msgExc);
        }
    }
}
