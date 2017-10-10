package com.linbit.drbdmanage.api.controller;

import com.linbit.ImplementationError;
import com.linbit.drbdmanage.CoreServices;
import com.linbit.drbdmanage.api.BaseApiCall;
import com.linbit.drbdmanage.core.Controller;
import com.linbit.drbdmanage.netcom.IllegalMessageStateException;
import com.linbit.drbdmanage.netcom.Message;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.proto.MsgDebugReplyOuterClass.MsgDebugReply;
import com.linbit.drbdmanage.security.AccessContext;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Debug API - make a peer a superuser
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class DebugMakeSuperuser extends BaseApiCall
{
    private Controller ctrl;
    private CoreServices coreSvcs;

    public DebugMakeSuperuser(
        Controller ctrlRef,
        CoreServices coreSvcsRef
    )
    {
        super(ctrlRef.getErrorReporter());
        ctrl = ctrlRef;
        coreSvcs = coreSvcsRef;
    }

    @Override
    public String getName()
    {
        return DebugMakeSuperuser.class.getSimpleName();
    }

    @Override
    public void executeImpl(
        AccessContext accCtx,
        Message msg,
        int msgId,
        InputStream msgDataIn,
        Peer client
    )
        throws IOException
    {
        try
        {
            ByteArrayOutputStream replyOut = new ByteArrayOutputStream();
            writeMsgHeader(replyOut, msgId, "DebugReply");

            ByteArrayOutputStream debugErr = new ByteArrayOutputStream();
            MsgDebugReply.Builder msgDbgReplyBld = MsgDebugReply.newBuilder();

            if (ctrl.debugMakePeerPrivileged(client))
            {
                msgDbgReplyBld.addDebugOut("Security context has been changed");
            }
            else
            {
                msgDbgReplyBld.addDebugErr("Request to change the security context has been denied");
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
}
