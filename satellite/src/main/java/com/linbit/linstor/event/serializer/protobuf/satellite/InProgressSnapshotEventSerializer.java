package com.linbit.linstor.event.serializer.protobuf.satellite;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.core.SnapshotState;
import com.linbit.linstor.event.LinstorEvent;
import com.linbit.linstor.event.WatchableObject;
import com.linbit.linstor.event.satellite.InProgressSnapshotEvent;
import com.linbit.linstor.event.serializer.EventSerializer;
import com.linbit.linstor.event.serializer.protobuf.ProtobufEventSerializer;

import javax.inject.Inject;
import javax.inject.Singleton;

@ProtobufEventSerializer(
    eventName = InternalApiConsts.EVENT_IN_PROGRESS_SNAPSHOT,
    objectType = WatchableObject.SNAPSHOT
)
@Singleton
public class InProgressSnapshotEventSerializer implements EventSerializer, EventSerializer.Serializer<SnapshotState>
{
    private final CtrlStltSerializer ctrlStltSerializer;
    private final InProgressSnapshotEvent inProgressSnapshotEvent;

    @Inject
    public InProgressSnapshotEventSerializer(
        CtrlStltSerializer ctrlStltSerializerRef,
        InProgressSnapshotEvent inProgressSnapshotEventRef
    )
    {
        ctrlStltSerializer = ctrlStltSerializerRef;
        inProgressSnapshotEvent = inProgressSnapshotEventRef;
    }

    @Override
    public Serializer get()
    {
        return this;
    }

    @Override
    public byte[] writeEventValue(SnapshotState snapshotState)
    {
        return ctrlStltSerializer.headerlessBuilder().inProgressSnapshotEvent(snapshotState).build();
    }

    @Override
    public LinstorEvent<SnapshotState> getEvent()
    {
        return inProgressSnapshotEvent.get();
    }
}
