package com.linbit.linstor.event.serializer.protobuf.controller;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlClientSerializer;
import com.linbit.linstor.event.LinstorEvent;
import com.linbit.linstor.event.WatchableObject;
import com.linbit.linstor.event.controller.SnapshotDeploymentEvent;
import com.linbit.linstor.event.serializer.EventSerializer;
import com.linbit.linstor.event.serializer.protobuf.ProtobufEventSerializer;

import javax.inject.Inject;
import javax.inject.Singleton;

@ProtobufEventSerializer(
    eventName = ApiConsts.EVENT_SNAPSHOT_DEPLOYMENT,
    objectType = WatchableObject.SNAPSHOT_DEFINITION
)
@Singleton
public class SnapshotDeploymentEventSerializer implements EventSerializer, EventSerializer.Serializer<ApiCallRc>
{
    private final CtrlClientSerializer ctrlClientSerializer;
    private final SnapshotDeploymentEvent snapshotDeploymentEvent;

    @Inject
    public SnapshotDeploymentEventSerializer(
        CtrlClientSerializer ctrlClientSerializerRef,
        SnapshotDeploymentEvent snapshotDeploymentEventRef
    )
    {
        ctrlClientSerializer = ctrlClientSerializerRef;
        snapshotDeploymentEvent = snapshotDeploymentEventRef;
    }

    @Override
    public Serializer get()
    {
        return this;
    }

    @Override
    public byte[] writeEventValue(ApiCallRc apiCallRc)
    {
        return ctrlClientSerializer.headerlessBuilder().snapshotDeploymentEvent(apiCallRc).build();
    }

    @Override
    public LinstorEvent<ApiCallRc> getEvent()
    {
        return snapshotDeploymentEvent.get();
    }
}
