package com.linbit.linstor.api.protobuf.controller;

import com.google.inject.Inject;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.CtrlApiCallHandler;
import com.linbit.linstor.proto.javainternal.MsgIntObjectIdOuterClass.MsgIntObjectId;

import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

@ProtobufApiCall(
    name = InternalApiConsts.API_REQUEST_NODE,
    description = "Called by the satellite to request node update data"
)
public class IntRequestNode implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;

    @Inject
    public IntRequestNode(CtrlApiCallHandler apiCallHandlerRef)
    {
        apiCallHandler = apiCallHandlerRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgIntObjectId objId = MsgIntObjectId.parseDelimitedFrom(msgDataIn);
        UUID nodeUuid = UUID.fromString(objId.getUuid());
        String nodeName = objId.getName();

        apiCallHandler.handleNodeRequest(nodeUuid, nodeName);
    }

}
