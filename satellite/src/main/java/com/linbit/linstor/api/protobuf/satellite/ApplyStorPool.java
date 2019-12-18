package com.linbit.linstor.api.protobuf.satellite;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.pojo.StorPoolPojo;
import com.linbit.linstor.api.protobuf.ProtoDeserializationUtils;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.apicallhandler.satellite.StltApiCallHandler;
import com.linbit.linstor.proto.common.StorPoolDfnOuterClass;
import com.linbit.linstor.proto.common.StorPoolOuterClass;
import com.linbit.linstor.proto.javainternal.c2s.IntStorPoolOuterClass.IntStorPool;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntApplyStorPoolOuterClass.MsgIntApplyStorPool;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;

@ProtobufApiCall(
    name = InternalApiConsts.API_APPLY_STOR_POOL,
    description = "Applies storage pool update data"
)
@Singleton
public class ApplyStorPool implements ApiCall
{
    private final StltApiCallHandler apiCallHandler;
    private final ControllerPeerConnector controllerPeerConnector;

    @Inject
    public ApplyStorPool(
        StltApiCallHandler apiCallHandlerRef,
        ControllerPeerConnector controllerPeerConnectorRef
    )
    {
        apiCallHandler = apiCallHandlerRef;
        controllerPeerConnector = controllerPeerConnectorRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgIntApplyStorPool applyStorPool = MsgIntApplyStorPool.parseDelimitedFrom(msgDataIn);

        StorPoolPojo storPoolRaw = asStorPoolPojo(
            applyStorPool.getStorPool(),
            controllerPeerConnector.getLocalNode().getName().displayValue,
            applyStorPool.getFullSyncId(),
            applyStorPool.getUpdateId()
        );
        apiCallHandler.applyStorPoolChanges(storPoolRaw);
    }

    static StorPoolPojo asStorPoolPojo(
        IntStorPool intStorPool,
        String nodeName,
        long fullSyncId,
        long updateId
    )
    {
        StorPoolOuterClass.StorPool protoStorPool = intStorPool.getStorPool();
        StorPoolDfnOuterClass.StorPoolDfn protoStorPoolDfn = intStorPool.getStorPoolDfn();

        return new StorPoolPojo(
            UUID.fromString(protoStorPool.getStorPoolUuid()),
            UUID.fromString(protoStorPool.getNodeUuid()),
            nodeName,
            protoStorPool.getStorPoolName(),
            UUID.fromString(protoStorPool.getStorPoolDfnUuid()),
            ProtoDeserializationUtils.parseDeviceProviderKind(protoStorPool.getProviderKind()),
            protoStorPool.getPropsMap(),
            protoStorPoolDfn.getPropsMap(),
            Collections.<String, String>emptyMap(),
            fullSyncId,
            updateId,
            protoStorPool.getFreeSpaceMgrName(),
            Optional.empty(), // free space
            Optional.empty(), // total space
            null,
            protoStorPool.getSnapshotSupported(),
            protoStorPool.getIsPmem()
        );
    }

}
