package com.linbit.linstor.api.protobuf;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.core.apicallhandler.StltApiCallHandler;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntApplyDeletedNodeOuterClass.MsgIntApplyDeletedNode;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
    name = InternalApiConsts.API_APPLY_NODE_DELETED,
    description = "Applies an update of a deleted node (ensuring the node is deleted)"
)
@Singleton
public class ApplyDeletedNode implements ApiCall
{
    private final StltApiCallHandler apiCallHandler;

    @Inject
    public ApplyDeletedNode(StltApiCallHandler apiCallHandlerRef)
    {
        apiCallHandler = apiCallHandlerRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgIntApplyDeletedNode nodeDeletedData = MsgIntApplyDeletedNode.parseDelimitedFrom(msgDataIn);
        apiCallHandler.applyDeletedNodeChange(
            nodeDeletedData.getNodeName(),
            nodeDeletedData.getFullSyncId(),
            nodeDeletedData.getUpdateId()
        );
    }
}
