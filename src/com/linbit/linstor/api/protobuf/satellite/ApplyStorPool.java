package com.linbit.linstor.api.protobuf.satellite;

import java.io.IOException;
import java.io.InputStream;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.pojo.StorPoolPojo;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.Satellite;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.javainternal.MsgIntStorPoolDataOuterClass.MsgIntStorPoolData;
import com.linbit.linstor.security.AccessContext;

@ProtobufApiCall
public class ApplyStorPool extends BaseProtoApiCall
{
    private Satellite satellite;

    public ApplyStorPool(Satellite satellite)
    {
        super(satellite.getErrorReporter());
        this.satellite = satellite;
    }

    @Override
    public String getName()
    {
        return InternalApiConsts.API_APPLY_STOR_POOL;
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

        StorPoolPojo storPoolRaw = asStorPoolPojo(storPoolData);
        satellite.getApiCallHandler().deployStorPool(storPoolRaw);
    }

    static StorPoolPojo asStorPoolPojo(MsgIntStorPoolData storPoolData)
    {
        StorPoolPojo storPoolRaw = new StorPoolPojo(
            asUuid(storPoolData.getStorPoolUuid()),
            asUuid(storPoolData.getNodeUuid()),
            storPoolData.getStorPoolName(),
            asUuid(storPoolData.getStorPoolDfnUuid()),
            storPoolData.getDriver(),
            asMap(storPoolData.getStorPoolPropsList()),
            asMap(storPoolData.getStorPoolDfnPropsList())
        );
        return storPoolRaw;
    }

}
