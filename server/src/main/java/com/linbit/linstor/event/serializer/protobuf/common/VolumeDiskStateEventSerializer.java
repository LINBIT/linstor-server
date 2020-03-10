package com.linbit.linstor.event.serializer.protobuf.common;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer;
import com.linbit.linstor.event.LinstorEvent;
import com.linbit.linstor.event.WatchableObject;
import com.linbit.linstor.event.common.VolumeDiskStateEvent;
import com.linbit.linstor.event.serializer.EventSerializer;
import com.linbit.linstor.event.serializer.protobuf.ProtobufEventSerializer;

import javax.inject.Inject;
import javax.inject.Singleton;

@ProtobufEventSerializer(
    eventName = InternalApiConsts.EVENT_VOLUME_DISK_STATE,
    objectType = WatchableObject.VOLUME
)
@Singleton
public class VolumeDiskStateEventSerializer implements EventSerializer, EventSerializer.Serializer<String>
{
    private final CommonSerializer commonSerializer;
    private final VolumeDiskStateEvent volumeDiskStateEvent;

    @Inject
    public VolumeDiskStateEventSerializer(
        CommonSerializer commonSerializerRef,
        VolumeDiskStateEvent volumeDiskStateEventRef
    )
    {
        commonSerializer = commonSerializerRef;
        volumeDiskStateEvent = volumeDiskStateEventRef;
    }

    @Override
    public Serializer get()
    {
        return this;
    }

    @Override
    public byte[] writeEventValue(String diskState)
    {
        return commonSerializer.headerlessBuilder().volumeDiskState(diskState).build();
    }

    @Override
    public LinstorEvent<String> getEvent()
    {
        return volumeDiskStateEvent.get();
    }
}
