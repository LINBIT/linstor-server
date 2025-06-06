package com.linbit.linstor.api.protobuf;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.DeviceManager;
import com.linbit.linstor.core.StltUpdateTrackerImpl;
import com.linbit.linstor.core.apicallhandler.ResponseSerializer;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntChangedDataOuterClass;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntChangedDataOuterClass.MsgIntChangedData;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;
import java.util.function.Function;

import reactor.core.publisher.Flux;

@ProtobufApiCall(
    name = InternalApiConsts.API_CHANGED_DATA,
    description = "Called by the controller to indicate that data was modified"
)
@Singleton
public class ChangedData implements ApiCallReactive
{
    private final DeviceManager deviceManager;
    private final ControllerPeerConnector controllerPeerConnector;
    private final ResponseSerializer responseSerializer;

    @Inject
    public ChangedData(
        DeviceManager deviceManagerRef,
        ControllerPeerConnector controllerPeerConnectorRef,
        ResponseSerializer responseSerializerRef
    )
    {
        deviceManager = deviceManagerRef;
        controllerPeerConnector = controllerPeerConnectorRef;
        responseSerializer = responseSerializerRef;
    }

    @Override
    public Flux<byte[]> executeReactive(InputStream msgDataIn)
        throws IOException
    {
        MsgIntChangedData protoData = MsgIntChangedData.parseDelimitedFrom(msgDataIn);

        StltUpdateTrackerImpl.AtomicUpdateHolder atomicUpdateHolder = new StltUpdateTrackerImpl.AtomicUpdateHolder();
        NodeName localNodeName = controllerPeerConnector.getLocalNodeName();

        try
        {
            for (MsgIntChangedDataOuterClass.ChangedResource protoRsc : protoData.getRscsList())
            {
                atomicUpdateHolder.putRsc(
                    UUID.fromString(protoRsc.getUuid()),
                    localNodeName,
                    new ResourceName(protoRsc.getName())
                );
            }
            for (MsgIntChangedDataOuterClass.ChangedSnapshot protoSnap : protoData.getSnapsList())
            {
                atomicUpdateHolder.putSnap(
                    UUID.fromString(protoSnap.getUuid()),
                    new ResourceName(protoSnap.getRscName()),
                    new SnapshotName(protoSnap.getSnapName())
                );
            }
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw new ImplementationError(
                "Controller sent an illegal name.",
                invalidNameExc
            );
        }

        return Flux.fromIterable(
            deviceManager.getUpdateTracker().updateData(atomicUpdateHolder)
        )
            .flatMap(Function.identity())
            .transform(responseSerializer::transform);
    }
}
