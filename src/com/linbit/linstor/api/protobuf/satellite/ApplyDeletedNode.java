package com.linbit.linstor.api.protobuf.satellite;

import java.io.IOException;
import java.io.InputStream;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.Satellite;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.javainternal.MsgIntNodeDeletedDataOuterClass.MsgIntNodeDeletedData;
import com.linbit.linstor.security.AccessContext;

@ProtobufApiCall
public class ApplyDeletedNode extends BaseProtoApiCall
{
    private final Satellite satellite;

    public ApplyDeletedNode(Satellite satellite)
    {
        super(satellite.getErrorReporter());
        this.satellite = satellite;
    }

    @Override
    public String getName()
    {
        return InternalApiConsts.API_APPLY_NODE_DELETED;
    }

    @Override
    public String getDescription()
    {
        return "Applies an update of a deleted node (ensuring the node is deleted)";
    }

    @Override
    protected void executeImpl(AccessContext accCtx, Message msg, int msgId, InputStream msgDataIn, Peer client)
        throws IOException
    {
        MsgIntNodeDeletedData nodeDeletedData = MsgIntNodeDeletedData.parseDelimitedFrom(msgDataIn);
        satellite.getApiCallHandler().applyDeletedNodeChange(
            nodeDeletedData.getNodeName()
        );
    }
}
