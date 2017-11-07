package com.linbit.drbdmanage.api.protobuf.controller;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import com.linbit.ImplementationError;
import com.linbit.drbdmanage.CoreServices;
import com.linbit.drbdmanage.api.ApiConsts;
import com.linbit.drbdmanage.api.protobuf.BaseProtoApiCall;
import com.linbit.drbdmanage.api.protobuf.ProtobufApiCall;
import com.linbit.drbdmanage.core.Controller;
import com.linbit.drbdmanage.core.DrbdManage;
import com.linbit.drbdmanage.netcom.IllegalMessageStateException;
import com.linbit.drbdmanage.netcom.Message;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.proto.internal.MsgDebugReplyOuterClass.MsgDebugReply;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.Privilege;

/**
 * Creates a debug console for the peer
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
@ProtobufApiCall
public class CreateDebugConsole extends BaseProtoApiCall
{
    private Controller      ctrl;
    private CoreServices    coreSvcs;

    public CreateDebugConsole(
        Controller ctrlRef,
        CoreServices coreSvcsRef
    )
    {
        super(coreSvcsRef.getErrorReporter());
        ctrl = ctrlRef;
        coreSvcs = coreSvcsRef;
    }

    @Override
    public String getName()
    {
        return ApiConsts.API_CRT_DBG_CNSL;
    }

    @Override
    public String getDescription()
    {
        return "Creates a debug console and attaches it to the peer connection";
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
            Message reply = client.createMessage();
            ByteArrayOutputStream replyOut = new ByteArrayOutputStream();
            writeProtoMsgHeader(replyOut, msgId, "DebugReply");

            MsgDebugReply.Builder msgDbgReplyBld = MsgDebugReply.newBuilder();
            try
            {
                // Create the debug console
                {
                    AccessContext privCtx = accCtx.clone();
                    privCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_SYS_ALL);
                    ctrl.createDebugConsole(privCtx, privCtx, client);
                }

                msgDbgReplyBld.addDebugOut(
                    DrbdManage.PROGRAM + ", Module " + Controller.MODULE + ", Release " + DrbdManage.VERSION
                );
                msgDbgReplyBld.addDebugOut("Debug console attached to peer connection");
            }
            catch (AccessDeniedException accessExc)
            {
                coreSvcs.getErrorReporter().reportError(accessExc);
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
