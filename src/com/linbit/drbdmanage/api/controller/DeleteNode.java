package com.linbit.drbdmanage.api.controller;

import java.io.IOException;
import java.io.InputStream;
import com.google.protobuf.InvalidProtocolBufferException;
import com.linbit.drbdmanage.ApiCallRc;
import com.linbit.drbdmanage.api.ApiConsts;
import com.linbit.drbdmanage.api.BaseApiCall;
import com.linbit.drbdmanage.core.Controller;
import com.linbit.drbdmanage.netcom.Message;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.proto.MsgDelNodeOuterClass.MsgDelNode;
import com.linbit.drbdmanage.security.AccessContext;

public class DeleteNode extends BaseApiCall
{
    private final Controller controller;

    public DeleteNode(Controller controllerRef)
    {
        super(controllerRef.getErrorReporter());
        controller = controllerRef;
    }

    @Override
    public String getName()
    {
        return ApiConsts.API_DEL_NODE;
    }

    @Override
    public void execute(
        AccessContext accCtx,
        Message msg,
        int msgId,
        InputStream msgDataIn,
        Peer client
    )
    {
        try
        {
            MsgDelNode msgDeleteNode = MsgDelNode.parseDelimitedFrom(msgDataIn);
//            System.out.println("received msgDelNode: ");
//            System.out.println("   " + msgDeleteNode.getNodeName());
//
//            System.out.println("deleting...");
            ApiCallRc apiCallRc = controller.getApiCallHandler().deleteNode(
                accCtx,
                client,
                msgDeleteNode.getNodeName()
            );

            super.answerApiCallRc(accCtx, client, msgId, apiCallRc);
        }
        catch (InvalidProtocolBufferException e)
        {
            // TODO: error reporting
            e.printStackTrace();
        }
        catch (IOException e)
        {
            // TODO: error reporting
            e.printStackTrace();
        }
    }

}
