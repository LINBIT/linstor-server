package com.linbit.drbdmanage.controllerapi;

import com.linbit.drbdmanage.commonapi.BaseApiCall;
import com.linbit.ImplementationError;
import com.linbit.drbdmanage.Controller;
import com.linbit.drbdmanage.CoreServices;
import com.linbit.drbdmanage.netcom.IllegalMessageStateException;
import com.linbit.drbdmanage.netcom.Message;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.netcom.TcpConnector;
import com.linbit.drbdmanage.proto.MsgDebugReplyOuterClass.MsgDebugReply;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.Privilege;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Creates a debug console for the peer
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class CreateDebugConsole extends BaseApiCall
{
    private Controller      ctrl;
    private CoreServices    coreSvcs;

    public CreateDebugConsole(
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
        return CreateDebugConsole.class.getSimpleName();
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
            Message reply = client.createMessage();
            ByteArrayOutputStream replyOut = new ByteArrayOutputStream();
            writeMsgHeader(replyOut, msgId, "DebugReply");

            MsgDebugReply.Builder msgDbgReplyBld = MsgDebugReply.newBuilder();
            try
            {
                // Create the debug console
                {
                    AccessContext privCtx = accCtx.clone();
                    privCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_SYS_ALL);
                    ctrl.createDebugConsole(privCtx, privCtx, client);
                }
                accCtx.getEffectivePrivs().disablePrivileges(Privilege.PRIV_SYS_ALL);
                accCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_OBJ_VIEW);

                msgDbgReplyBld.addDebugOut("Debug console created");
            } catch (AccessDeniedException accessExc)
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
            throw new ImplementationError(
                Message.class.getName() + " object returned by the " + Peer.class.getName() +
                " class has an illegal state",
                msgExc
            );
        }
        catch (IOException ioExc)
        {
            coreSvcs.getErrorReporter().reportError(ioExc);
        }
    }
}
