package com.linbit.drbdmanage.api.controller;

import java.io.IOException;
import java.io.InputStream;
import com.linbit.drbdmanage.ApiCallRc;
import com.linbit.drbdmanage.ApiConsts;
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
    public String getDescription()
    {
        return "Marks a node for deletion";
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
        MsgDelNode msgDeleteNode = MsgDelNode.parseDelimitedFrom(msgDataIn);
        ApiCallRc apiCallRc = controller.getApiCallHandler().deleteNode(
            accCtx,
            client,
            msgDeleteNode.getNodeName()
        );

        super.answerApiCallRc(accCtx, client, msgId, apiCallRc);
    }

}
