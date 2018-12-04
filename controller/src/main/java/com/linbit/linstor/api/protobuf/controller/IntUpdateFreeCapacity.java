package com.linbit.linstor.api.protobuf.controller;


import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.pojo.CapacityInfoPojo;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.proto.StorPoolFreeSpaceOuterClass.StorPoolFreeSpace;
import com.linbit.linstor.proto.javainternal.MsgIntUpdateFreeSpaceOuterClass.MsgIntUpdateFreeSpace;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@ProtobufApiCall(
    name = InternalApiConsts.API_UPDATE_FREE_CAPACITY,
    description = "Satellite successfully applied a storage pool"
)
@Singleton
public class IntUpdateFreeCapacity implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;

    @Inject
    public IntUpdateFreeCapacity(
        CtrlApiCallHandler apiCallHandlerRef
    )
    {
        apiCallHandler = apiCallHandlerRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgIntUpdateFreeSpace updateMsg = MsgIntUpdateFreeSpace.parseDelimitedFrom(msgDataIn);

        List<StorPoolFreeSpace> freeSpaceProto = updateMsg.getFreeSpaceList();
        apiCallHandler.updateRealFreeSpace(
            freeSpaceProto.stream().map(
                proto -> new CapacityInfoPojo(
                    UUID.fromString(proto.getStorPoolUuid()),
                    proto.getStorPoolName(),
                    proto.getFreeCapacity(),
                    proto.getTotalCapacity()
                )
            ).collect(Collectors.toList())
        );
    }
}
