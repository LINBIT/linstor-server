package com.linbit.drbdmanage.api.protobuf.satellite;

import java.io.IOException;
import java.io.InputStream;

import com.linbit.drbdmanage.InternalApiConsts;
import com.linbit.drbdmanage.api.protobuf.BaseProtoApiCall;
import com.linbit.drbdmanage.api.protobuf.ProtobufApiCall;
import com.linbit.drbdmanage.api.raw.StorPoolRawData;
import com.linbit.drbdmanage.core.Satellite;
import com.linbit.drbdmanage.netcom.Message;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.proto.javainternal.MsgIntStorPoolDataOuterClass.MsgIntStorPoolData;
import com.linbit.drbdmanage.security.AccessContext;

@ProtobufApiCall
public class StorPoolData extends BaseProtoApiCall
{
    private Satellite satellite;

    public StorPoolData(Satellite satellite)
    {
        super(satellite.getErrorReporter());
        this.satellite = satellite;
    }

    @Override
    public String getName()
    {
        return InternalApiConsts.API_STOR_POOL_DATA;
    }

    @Override
    public String getDescription()
    {
        return "Deployes the storpool";
    }

    @Override
    protected void executeImpl(AccessContext accCtx, Message msg, int msgId, InputStream msgDataIn, Peer client)
        throws IOException
    {
        MsgIntStorPoolData storPoolData = MsgIntStorPoolData.parseDelimitedFrom(msgDataIn);

        StorPoolRawData storPoolRaw = new StorPoolRawData(
            asUuid(storPoolData.getStorPoolUuid()),
            asUuid(storPoolData.getNodeUuid()),
            storPoolData.getStorPoolName(),
            asUuid(storPoolData.getStorPoolDfnUuid()),
            storPoolData.getDriver(),
            asMap(storPoolData.getStorPoolPropsList()),
            asMap(storPoolData.getStorPoolDfnPropsList())
        );
        satellite.getApiCallHandler().deployStorPool(storPoolRaw);
    }

}
