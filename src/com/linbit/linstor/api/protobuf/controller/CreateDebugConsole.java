package com.linbit.linstor.api.protobuf.controller;

import javax.inject.Inject;
import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ApiCallAnswerer;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.Controller;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.debug.DebugConsoleCreator;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.IllegalMessageStateException;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.javainternal.MsgDebugReplyOuterClass.MsgDebugReply;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.Privilege;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Creates a debug console for the peer
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
@ProtobufApiCall(
    name = ApiConsts.API_CRT_DBG_CNSL,
    description = "Creates a debug console and attaches it to the peer connection"
)
public class CreateDebugConsole implements ApiCall
{
    private final ErrorReporter errorReporter;
    private final ApiCallAnswerer apiCallAnswerer;
    private final DebugConsoleCreator debugConsoleCreator;
    private final Peer client;
    private final AccessContext accCtx;

    @Inject
    public CreateDebugConsole(
        ErrorReporter errorReporterRef,
        ApiCallAnswerer apiCallAnswererRef,
        DebugConsoleCreator debugConsoleCreatorRef,
        Peer clientRef,
        @PeerContext AccessContext accCtxRef
    )
    {
        errorReporter = errorReporterRef;
        apiCallAnswerer = apiCallAnswererRef;
        debugConsoleCreator = debugConsoleCreatorRef;
        client = clientRef;
        accCtx = accCtxRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        try
        {
            Message reply = client.createMessage();
            ByteArrayOutputStream replyOut = new ByteArrayOutputStream();
            apiCallAnswerer.writeProtoMsgHeader(replyOut, "DebugReply");

            MsgDebugReply.Builder msgDbgReplyBld = MsgDebugReply.newBuilder();
            try
            {
                // Create the debug console
                {
                    AccessContext privCtx = accCtx.clone();
                    privCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_SYS_ALL);
                    debugConsoleCreator.createDebugConsole(privCtx, privCtx, client);
                }

                msgDbgReplyBld.addDebugOut(
                    LinStor.PROGRAM + ", Module " + Controller.MODULE
                );
                msgDbgReplyBld.addDebugOut(
                    "Version: " + LinStor.VERSION_INFO_PROVIDER.getVersion() + " " +
                    "(" + LinStor.VERSION_INFO_PROVIDER.getGitCommitId() + ")" + " - " +
                    LinStor.VERSION_INFO_PROVIDER.getBuildTime()
                );
                msgDbgReplyBld.addDebugOut("Debug console attached to peer connection");
            }
            catch (AccessDeniedException accessExc)
            {
                errorReporter.reportError(accessExc);
                msgDbgReplyBld.addDebugErr(
                    "Error:\n" +
                    "    The request to create a debug console was denied.\n" +
                    "Cause:    \n" +
                    accessExc.getMessage() +
                    "\n"
                );
            }

            {
                MsgDebugReply dbgReply = msgDbgReplyBld.build();
                dbgReply.writeDelimitedTo(replyOut);
                reply.setData(replyOut.toByteArray());
            }
            client.sendMessage(reply);
        }
        catch (IllegalMessageStateException msgExc)
        {
            errorReporter.reportError(
                new ImplementationError(
                    Message.class.getName() + " object returned by the " + Peer.class.getName() +
                    " class has an illegal state",
                    msgExc
                )
            );
        }
    }
}
