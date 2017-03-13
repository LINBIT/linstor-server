package com.linbit.drbdmanage.proto;

import com.linbit.ErrorCheck;
import com.linbit.drbdmanage.Controller;
import com.linbit.drbdmanage.CoreServices;
import com.linbit.drbdmanage.netcom.IllegalMessageStateException;
import com.linbit.drbdmanage.netcom.Message;
import com.linbit.drbdmanage.netcom.MessageProcessor;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.netcom.TcpConnector;

/**
 * Dispatcher for received messages
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class CtlMessageProcessor implements MessageProcessor
{
    private Controller controller;
    private CoreServices coreSvcs;

    public CtlMessageProcessor(
        Controller controllerRef,
        CoreServices coreSvcsRef
    )
    {
        ErrorCheck.ctorNotNull(CtlMessageProcessor.class, Controller.class, controllerRef);
        ErrorCheck.ctorNotNull(CtlMessageProcessor.class, CoreServices.class, coreSvcsRef);
        controller = controllerRef;
        coreSvcs = coreSvcsRef;
    }

    @Override
    public void processMessage(Message msg, TcpConnector connector, Peer client)
    {
        try
        {
            byte[] msgData = msg.getData();

        }
        catch (IllegalMessageStateException msgExc)
        {
            coreSvcs.getErrorReporter().reportError(msgExc);
        }
    }
}
