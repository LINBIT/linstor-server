package com.linbit.linstor.api.protobuf.satellite;

import java.io.IOException;
import java.io.InputStream;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.pojo.StorPoolPojo;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.api.protobuf.ProtoMapUtils;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.Satellite;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.javainternal.MsgIntStorPoolDataOuterClass.MsgIntStorPoolData;
import com.linbit.linstor.security.AccessContext;

import java.util.Collections;
import java.util.UUID;

@ProtobufApiCall
public class ApplyStorPool extends BaseProtoApiCall
{
    private final Satellite satellite;

    public ApplyStorPool(Satellite satelliteRef)
    {
        super(satelliteRef.getErrorReporter());
        satellite = satelliteRef;
    }

    @Override
    public String getName()
    {
        return InternalApiConsts.API_APPLY_STOR_POOL;
    }

    @Override
    public String getDescription()
    {
        return "Applies storage pool update data";
    }

    @Override
    protected void executeImpl(AccessContext accCtx, Message msg, int msgId, InputStream msgDataIn, Peer client)
        throws IOException
    {
        MsgIntStorPoolData storPoolData = MsgIntStorPoolData.parseDelimitedFrom(msgDataIn);

        StorPoolPojo storPoolRaw = asStorPoolPojo(
            storPoolData,
            satellite.getLocalNode().getName().displayValue
        );
        satellite.getApiCallHandler().applyStorPoolChanges(storPoolRaw);
    }

    static StorPoolPojo asStorPoolPojo(MsgIntStorPoolData storPoolData, String nodeName)
    {
        StorPoolPojo storPoolRaw = new StorPoolPojo(
            UUID.fromString(storPoolData.getStorPoolUuid()),
            UUID.fromString(storPoolData.getNodeUuid()),
            nodeName,
            storPoolData.getStorPoolName(),
            UUID.fromString(storPoolData.getStorPoolDfnUuid()),
            storPoolData.getDriver(),
            ProtoMapUtils.asMap(storPoolData.getStorPoolPropsList()),
            ProtoMapUtils.asMap(storPoolData.getStorPoolDfnPropsList()),
            null,
            Collections.<String, String>emptyMap(),
            storPoolData.getFullSyncId(),
            storPoolData.getUpdateId()
        );
        return storPoolRaw;
    }

}
