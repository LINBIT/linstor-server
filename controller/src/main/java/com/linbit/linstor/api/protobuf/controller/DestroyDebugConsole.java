package com.linbit.linstor.api.protobuf.controller;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ApiCallAnswerer;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.debug.DebugConsoleCreator;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.IllegalMessageStateException;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.javainternal.MsgDebugReplyOuterClass.MsgDebugReply;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Destroys (frees) the peer's debug console
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
@ProtobufApiCall(
    name = ApiConsts.API_DSTR_DBG_CNSL,
    description = "Detaches a debug console from a peer connection and destroys the debug console object",
    transactional = false
)
@Singleton
public class DestroyDebugConsole implements ApiCall
{
    private final ErrorReporter errorReporter;
    private final ApiCallAnswerer apiCallAnswerer;
    private final DebugConsoleCreator debugConsoleCreator;
    private final Provider<Peer> clientProvider;
    private final Provider<AccessContext> accCtxProvider;

    @Inject
    public DestroyDebugConsole(
        ErrorReporter errorReporterRef,
        ApiCallAnswerer apiCallAnswererRef,
        DebugConsoleCreator debugConsoleCreatorRef,
        Provider<Peer> clientProviderRef,
        @PeerContext Provider<AccessContext> accCtxProviderRef
    )
    {
        errorReporter = errorReporterRef;
        apiCallAnswerer = apiCallAnswererRef;
        debugConsoleCreator = debugConsoleCreatorRef;
        clientProvider = clientProviderRef;
        accCtxProvider = accCtxProviderRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        try
        {
            Peer client = clientProvider.get();
            Message reply = client.createMessage();
            ByteArrayOutputStream replyOut = new ByteArrayOutputStream();
            apiCallAnswerer.writeAnswerHeader(replyOut, "DebugReply");

            MsgDebugReply.Builder msgDbgReplyBld = MsgDebugReply.newBuilder();
            try
            {
                debugConsoleCreator.destroyDebugConsole(accCtxProvider.get(), client);

                msgDbgReplyBld.addDebugOut("Debug console destroyed");
            }
            catch (AccessDeniedException accessExc)
            {
                errorReporter.reportError(accessExc);
                msgDbgReplyBld.addDebugErr(
                    "Error:\n" +
                    "    The request to destroy the debug console was denied.\n" +
                    "Cause:\n    " +
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
