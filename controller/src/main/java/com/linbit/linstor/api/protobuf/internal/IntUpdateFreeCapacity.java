package com.linbit.linstor.api.protobuf.internal;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.pojo.CapacityInfoPojo;
import com.linbit.linstor.api.protobuf.ProtoDeserializationUtils;
import com.linbit.linstor.api.protobuf.ProtoUuidUtils;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.internal.StorPoolInternalCallHandler;
import com.linbit.linstor.proto.common.StorPoolFreeSpaceOuterClass.StorPoolFreeSpace;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntUpdateFreeSpaceOuterClass.MsgIntUpdateFreeSpace;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.stream.Collectors;

@ProtobufApiCall(
    name = InternalApiConsts.API_UPDATE_FREE_CAPACITY,
    description = "Satellite successfully applied a storage pool"
)
@Singleton
public class IntUpdateFreeCapacity implements ApiCall
{
    private final StorPoolInternalCallHandler storPoolInternalCallHandler;

    @Inject
    public IntUpdateFreeCapacity(
        StorPoolInternalCallHandler apiCallHandlerRef
    )
    {
        storPoolInternalCallHandler = apiCallHandlerRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgIntUpdateFreeSpace updateMsg = MsgIntUpdateFreeSpace.parseDelimitedFrom(msgDataIn);

        List<StorPoolFreeSpace> freeSpaceProto = updateMsg.getFreeSpaceList();
        storPoolInternalCallHandler.updateRealFreeSpace(
            freeSpaceProto.stream().map(
                proto -> new CapacityInfoPojo(
                    ProtoUuidUtils.deserialize(proto.getStorPoolUuid()),
                    proto.getStorPoolName(),
                    proto.getFreeCapacity(),
                    proto.getTotalCapacity(),
                    ProtoDeserializationUtils.parseApiCallRcList(proto.getErrorsList())
                )
            ).collect(Collectors.toList())
        );
    }
}
