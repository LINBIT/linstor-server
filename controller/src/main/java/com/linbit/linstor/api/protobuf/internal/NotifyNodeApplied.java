package com.linbit.linstor.api.protobuf.internal;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.protobuf.ProtoUuidUtils;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.internal.NodeInternalCallHandler;
import com.linbit.linstor.proto.javainternal.IntObjectIdOuterClass.IntObjectId;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntApplyNodeSuccessOuterClass.MsgIntApplyNodeSuccess;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
    name = InternalApiConsts.API_NOTIFY_NODE_APPLIED,
    description = "Called by the satellite to notify the controller of successful " +
        "node creation, modification or deletion"
)
@Singleton
public class NotifyNodeApplied implements ApiCall
{
    private final NodeInternalCallHandler nodeInternalCallHandler;

    @Inject
    public NotifyNodeApplied(
        NodeInternalCallHandler nodeInternalCallHandlerRef
    )
    {
        nodeInternalCallHandler = nodeInternalCallHandlerRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        IntObjectId intObjectId = MsgIntApplyNodeSuccess.parseDelimitedFrom(msgDataIn).getNodeId();

        nodeInternalCallHandler.handleNodeRequest(
            ProtoUuidUtils.deserialize(intObjectId.getUuid()),
            intObjectId.getName()
        );
    }
}
