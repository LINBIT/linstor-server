package com.linbit.linstor.event.serializer.protobuf.common;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer;
import com.linbit.linstor.event.LinstorEvent;
import com.linbit.linstor.event.WatchableObject;
import com.linbit.linstor.event.common.ResourceStateEvent;
import com.linbit.linstor.event.common.ResourceState;
import com.linbit.linstor.event.serializer.EventSerializer;
import com.linbit.linstor.event.serializer.protobuf.ProtobufEventSerializer;

import javax.inject.Inject;
import javax.inject.Singleton;

@ProtobufEventSerializer(
    eventName = InternalApiConsts.EVENT_RESOURCE_STATE,
    objectType = WatchableObject.RESOURCE
)
@Singleton
public class ResourceStateEventSerializer implements EventSerializer, EventSerializer.Serializer<ResourceState>
{
    private final CommonSerializer commonSerializer;
    private final ResourceStateEvent resourceStateEvent;

    @Inject
    public ResourceStateEventSerializer(
        CommonSerializer commonSerializerRef,
        ResourceStateEvent resourceStateEventRef
    )
    {
        commonSerializer = commonSerializerRef;
        resourceStateEvent = resourceStateEventRef;
    }

    @Override
    public Serializer get()
    {
        return this;
    }

    @Override
    public byte[] writeEventValue(ResourceState resourceState)
    {
        return commonSerializer.headerlessBuilder().resourceStateEvent(resourceState).build();
    }

    @Override
    public LinstorEvent getEvent()
    {
        return resourceStateEvent.get();
    }
}
