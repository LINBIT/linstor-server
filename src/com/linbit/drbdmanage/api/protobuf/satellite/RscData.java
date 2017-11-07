package com.linbit.drbdmanage.api.protobuf.satellite;

import java.io.IOException;
import java.io.InputStream;

import com.linbit.drbdmanage.InternalApiConsts;
import com.linbit.drbdmanage.api.protobuf.BaseProtoApiCall;
import com.linbit.drbdmanage.api.protobuf.ProtobufApiCall;
import com.linbit.drbdmanage.core.Satellite;
import com.linbit.drbdmanage.netcom.Message;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.proto.javainternal.MsgIntRscDataOuterClass.MsgIntRscData;
import com.linbit.drbdmanage.security.AccessContext;

@ProtobufApiCall
public class RscData extends BaseProtoApiCall
{
    public RscData(Satellite satellite)
    {
        super(satellite.getErrorReporter());
    }

    @Override
    public String getName()
    {
        return InternalApiConsts.API_RSC_DATA;
    }

    @Override
    public String getDescription()
    {
        return "Deployes the resources";
    }

    @Override
    protected void executeImpl(AccessContext accCtx, Message msg, int msgId, InputStream msgDataIn, Peer client)
        throws IOException
    {
        MsgIntRscData rscData = MsgIntRscData.parseDelimitedFrom(msgDataIn);
    }
}
