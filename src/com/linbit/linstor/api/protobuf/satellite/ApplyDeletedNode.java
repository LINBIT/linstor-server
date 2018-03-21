package com.linbit.linstor.api.protobuf.satellite;

import javax.inject.Inject;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.StltApiCallHandler;
import com.linbit.linstor.proto.javainternal.MsgIntNodeDeletedDataOuterClass.MsgIntNodeDeletedData;

import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
    name = InternalApiConsts.API_APPLY_NODE_DELETED,
    description = "Applies an update of a deleted node (ensuring the node is deleted)"
)
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
        MsgIntNodeDeletedData nodeDeletedData = MsgIntNodeDeletedData.parseDelimitedFrom(msgDataIn);
        apiCallHandler.applyDeletedNodeChange(
            nodeDeletedData.getNodeName(),
            nodeDeletedData.getFullSyncId(),
            nodeDeletedData.getUpdateId()
        );
    }
}
