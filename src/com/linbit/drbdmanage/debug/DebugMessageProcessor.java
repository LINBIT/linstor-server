package com.linbit.drbdmanage.debug;

import com.linbit.drbdmanage.CoreServices;
import com.linbit.drbdmanage.netcom.IllegalMessageStateException;
import com.linbit.drbdmanage.netcom.Message;
import com.linbit.drbdmanage.netcom.MessageProcessor;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.netcom.TcpConnector;

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
            HexViewer.dumpByteArray(msg.getData());
        }
        catch (IllegalMessageStateException msgExc)
        {
            coreSvcs.getErrorReporter().reportError(msgExc);
        }
    }
}
