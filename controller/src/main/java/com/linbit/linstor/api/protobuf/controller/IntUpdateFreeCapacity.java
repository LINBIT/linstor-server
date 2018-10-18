package com.linbit.linstor.api.protobuf.controller;

import javax.inject.Inject;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.pojo.CapacityInfoPojo;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.proto.StorPoolFreeSpaceOuterClass.StorPoolFreeSpace;
import com.linbit.linstor.proto.javainternal.MsgIntApplyStorPoolSuccessOuterClass.MsgIntApplyStorPoolSuccess;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.UUID;

@ProtobufApiCall(
    name = InternalApiConsts.API_UPDATE_FREE_CAPACITY,
    description = "Satellite successfully applied a storage pool"
)
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
        MsgIntApplyStorPoolSuccess successMsg = MsgIntApplyStorPoolSuccess.parseDelimitedFrom(msgDataIn);

        StorPoolFreeSpace freeSpaceProto = successMsg.getFreeSpace();
        apiCallHandler.updateRealFreeSpace(
            Arrays.asList(
                new CapacityInfoPojo(
                    UUID.fromString(freeSpaceProto.getStorPoolUuid()),
                    freeSpaceProto.getStorPoolName(),
                    freeSpaceProto.getFreeCapacity(),
                    freeSpaceProto.getTotalCapacity()
                )
            )
        );
    }

}
