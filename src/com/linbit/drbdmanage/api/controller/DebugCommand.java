package com.linbit.drbdmanage.api.controller;

import com.linbit.ImplementationError;
import com.linbit.drbdmanage.CoreServices;
import com.linbit.drbdmanage.api.BaseApiCall;
import com.linbit.drbdmanage.core.Controller;
import com.linbit.drbdmanage.ControllerPeerCtx;
import com.linbit.drbdmanage.debug.DebugConsole;
import com.linbit.drbdmanage.netcom.IllegalMessageStateException;
import com.linbit.drbdmanage.netcom.Message;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.proto.internal.MsgDebugCommandOuterClass.MsgDebugCommand;
import com.linbit.drbdmanage.proto.internal.MsgDebugReplyOuterClass.MsgDebugReply;
import com.linbit.drbdmanage.security.AccessContext;

import java.io.*;
import java.util.Deque;
import java.util.LinkedList;

/**
 * Pipes a debug command from the peer to the server and the
 * command's information and error reply back to the peer
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class DebugCommand extends BaseApiCall
{
    private Controller ctrl;
    private CoreServices coreSvcs;

    public DebugCommand(
        Controller ctrlRef,
        CoreServices coreSvcsRef
    )
    {
        super(ctrlRef.getErrorReporter());
        ctrl = ctrlRef;
        coreSvcs = coreSvcsRef;
    }

    @Override
    public String getDescription()
    {
        return "Submits a debug command to an active debug console attached to the peer.\n" +
            "The debug console typically answers with a DebugReply message.\n";
    }

    @Override
    public String getName()
    {
        return DebugCommand.class.getSimpleName();
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
            ByteArrayOutputStream replyOut = new ByteArrayOutputStream();
            writeMsgHeader(replyOut, msgId, "DebugReply");
            ByteArrayOutputStream debugErr = new ByteArrayOutputStream();
            MsgDebugReply.Builder msgDbgReplyBld = MsgDebugReply.newBuilder();

            ControllerPeerCtx peerContext = (ControllerPeerCtx) client.getAttachment();
            DebugConsole dbgConsole = peerContext.getDebugConsole();
            if (dbgConsole != null)
            {
                MsgDebugCommand msgDbgCmd = MsgDebugCommand.parseDelimitedFrom(msgDataIn);
                ByteArrayInputStream cmdIn = new ByteArrayInputStream(msgDbgCmd.getCmdLine().getBytes());

                ByteArrayOutputStream debugOut = new ByteArrayOutputStream();
                dbgConsole.streamsConsole(
                    "",
                    cmdIn,
                    new PrintStream(debugOut),
                    new PrintStream(debugErr),
                    false
                );

                String[] debugOutData = toLinesArray(debugOut.toByteArray());
                String[] debugErrData = toLinesArray(debugErr.toByteArray());

                for (String line : debugOutData)
                {
                    msgDbgReplyBld.addDebugOut(line);
                }
                for (String line : debugErrData)
                {
                    msgDbgReplyBld.addDebugErr(line);
                }
            }
            else
            {
                msgDbgReplyBld.addDebugErr(
                    "There is no active debug console for this peer"
                );
            }

            MsgDebugReply msgDbgReply = msgDbgReplyBld.build();
            Message reply = client.createMessage();
            {
                msgDbgReply.writeDelimitedTo(replyOut);
                reply.setData(replyOut.toByteArray());
            }
            client.sendMessage(reply);
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

    private String[] toLinesArray(byte[] text)
    {
        Deque<String> debugOutLines = new LinkedList<>();
        int offset = 0;
        for (int idx = 0; idx < text.length; ++idx)
        {
            if (text[idx] == '\n')
            {
                debugOutLines.add(new String(text, offset, idx - offset));
                offset = idx + 1;
            }
        }
        String[] strArray = new String[debugOutLines.size()];
        return debugOutLines.toArray(strArray);
    }
}
