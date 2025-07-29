package com.linbit.linstor.api.protobuf.internal;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.pojo.CapacityInfoPojo;
import com.linbit.linstor.api.protobuf.ProtoDeserializationUtils;
import com.linbit.linstor.api.protobuf.ProtoUuidUtils;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.internal.StorPoolInternalCallHandler;
import com.linbit.linstor.proto.common.StorPoolFreeSpaceOuterClass.StorPoolFreeSpace;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntApplyStorPoolSuccessOuterClass.MsgIntApplyStorPoolSuccess;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
    name = InternalApiConsts.API_NOTIFY_STOR_POOL_APPLIED,
    description = "Called by the satellite to notify the controller of successful " +
        "creation, modification or deletion of a storage pool"
)
@Singleton
public class NotifyStorPoolApplied implements ApiCall
{
    private final StorPoolInternalCallHandler storPoolInternalCallHandler;

    @Inject
    public NotifyStorPoolApplied(StorPoolInternalCallHandler apiCallHandlerRef)
    {
        storPoolInternalCallHandler = apiCallHandlerRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgIntApplyStorPoolSuccess protoMsg = MsgIntApplyStorPoolSuccess.parseDelimitedFrom(msgDataIn);

        String storPoolName = protoMsg.getStorPoolName();
        boolean supportsSnapshots = protoMsg.getSupportsSnapshots();

        StorPoolFreeSpace protoFreeSpace = protoMsg.getFreeSpace();
        storPoolInternalCallHandler.handleStorPoolApplied(
            storPoolName,
            supportsSnapshots,
            new CapacityInfoPojo(
                ProtoUuidUtils.deserialize(protoFreeSpace.getStorPoolUuid()),
                protoFreeSpace.getStorPoolName(),
                protoFreeSpace.getFreeCapacity(),
                protoFreeSpace.getTotalCapacity(),
                ProtoDeserializationUtils.parseApiCallRcList(protoFreeSpace.getErrorsList())
            )
        );
    }
}
