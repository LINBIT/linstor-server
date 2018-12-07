package com.linbit.linstor.api.protobuf.satellite;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.pojo.StorPoolPojo;
import com.linbit.linstor.api.protobuf.ProtoMapUtils;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.apicallhandler.satellite.StltApiCallHandler;
import com.linbit.linstor.proto.javainternal.MsgIntStorPoolDataOuterClass.MsgIntStorPoolData;

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
        MsgIntStorPoolData storPoolData = MsgIntStorPoolData.parseDelimitedFrom(msgDataIn);

        StorPoolPojo storPoolRaw = asStorPoolPojo(
            storPoolData,
            controllerPeerConnector.getLocalNode().getName().displayValue
        );
        apiCallHandler.applyStorPoolChanges(storPoolRaw);
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
            null, // List<Vlmapi> vlmRefs
            Collections.<String, String>emptyMap(),
            storPoolData.getFullSyncId(),
            storPoolData.getUpdateId(),
            storPoolData.getFreeSpaceMgrName(),
            Optional.empty(), // free space
            Optional.empty() // total space
        );
        return storPoolRaw;
    }

}
