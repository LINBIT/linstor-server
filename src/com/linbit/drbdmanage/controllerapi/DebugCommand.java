package com.linbit.drbdmanage.controllerapi;

import com.linbit.ImplementationError;
import com.linbit.drbdmanage.Controller;
import com.linbit.drbdmanage.CoreServices;
import com.linbit.drbdmanage.CtlPeerContext;
import com.linbit.drbdmanage.debug.DebugConsole;
import com.linbit.drbdmanage.netcom.IllegalMessageStateException;
import com.linbit.drbdmanage.netcom.Message;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.netcom.TcpConnector;
import com.linbit.drbdmanage.proto.MsgDebugCommandOuterClass.MsgDebugCommand;
import com.linbit.drbdmanage.proto.MsgDebugReplyOuterClass.MsgDebugReply;
import com.linbit.drbdmanage.security.AccessContext;

import java.io.*;
import java.util.Deque;
import java.util.LinkedList;
import java.util.StringTokenizer;

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
        ctrl = ctrlRef;
        coreSvcs = coreSvcsRef;
    }

    @Override
    public String getName()
    {
        return DebugCommand.class.getSimpleName();
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
            ByteArrayOutputStream replyOut = new ByteArrayOutputStream();
            writeMsgHeader(replyOut, msgId, "DebugReply");
            ByteArrayOutputStream debugErr = new ByteArrayOutputStream();
            MsgDebugReply.Builder msgDbgReplyBld = MsgDebugReply.newBuilder();

            CtlPeerContext peerContext = (CtlPeerContext) client.getAttachment();
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

                String[] debugOutData = toLinesArray(debugOut.toString());
                String[] debugErrData = toLinesArray(debugErr.toString());

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
        catch (IOException ioExc)
        {
            ioExc.printStackTrace(System.err);
            coreSvcs.getErrorReporter().reportError(ioExc);
        }
        catch (IllegalMessageStateException msgExc)
        {
            msgExc.printStackTrace(System.err);
            throw new ImplementationError(
                Message.class.getName() + " object returned by the " + Peer.class.getName() +
                " class has an illegal state",
                msgExc
            );
        }
    }

    private String[] toLinesArray(String text)
    {
        Deque<String> debugOutLines = new LinkedList<>();
        StringTokenizer debugOutTokens = new StringTokenizer(text, "\n");
        while (debugOutTokens.hasMoreTokens())
        {
            debugOutLines.add(debugOutTokens.nextToken());
        }
        String[] strArray = new String[debugOutLines.size()];
        return debugOutLines.toArray(strArray);
    }
}
