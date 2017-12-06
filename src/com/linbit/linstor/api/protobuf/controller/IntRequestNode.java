package com.linbit.linstor.api.protobuf.controller;

import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.Controller;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.javainternal.MsgIntObjectIdOuterClass.MsgIntObjectId;
import com.linbit.linstor.security.AccessContext;
import com.linbit.utils.UuidUtils;

@ProtobufApiCall
public class IntRequestNode extends BaseProtoApiCall
{
    private final Controller controller;

    public IntRequestNode(Controller controller)
    {
        super(controller.getErrorReporter());
        this.controller = controller;
    }

    @Override
    public String getName()
    {
        return InternalApiConsts.API_REQUEST_NODE;
    }

    @Override
    public String getDescription()
    {
        return "Requests the information about the given node";
    }

    @Override
    protected void executeImpl(AccessContext accCtx, Message msg, int msgId, InputStream msgDataIn, Peer client)
        throws IOException
    {
        MsgIntObjectId objId = MsgIntObjectId.parseDelimitedFrom(msgDataIn);
        UUID nodeUuid = UuidUtils.asUuid(objId.getUuid().toByteArray());
        String nodeName = objId.getName();

        controller.getApiCallHandler().handleNodeRequest(client, msgId, nodeUuid, nodeName);
    }

}
