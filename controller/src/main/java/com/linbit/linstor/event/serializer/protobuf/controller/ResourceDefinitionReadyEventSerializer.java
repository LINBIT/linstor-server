package com.linbit.linstor.event.serializer.protobuf.controller;

import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer;
import com.linbit.linstor.event.LinstorEvent;
import com.linbit.linstor.event.WatchableObject;
import com.linbit.linstor.event.controller.ResourceDefinitionReadyEvent;
import com.linbit.linstor.event.controller.ResourceDefinitionReadyEventData;
import com.linbit.linstor.event.serializer.EventSerializer;
import com.linbit.linstor.event.serializer.protobuf.ProtobufEventSerializer;

import javax.inject.Inject;
import javax.inject.Singleton;

@ProtobufEventSerializer(
    eventName = ApiConsts.EVENT_RESOURCE_DEFINITION_READY,
    objectType = WatchableObject.RESOURCE_DEFINITION
)
@Singleton
public class ResourceDefinitionReadyEventSerializer
    implements EventSerializer, EventSerializer.Serializer<ResourceDefinitionReadyEventData>
{
    private final CommonSerializer commonSerializer;
    private final ResourceDefinitionReadyEvent resourceDefinitionReadyEvent;

    @Inject
    public ResourceDefinitionReadyEventSerializer(
        CommonSerializer commonSerializerRef,
        ResourceDefinitionReadyEvent resourceDefinitionReadyEventRef
    )
    {
        commonSerializer = commonSerializerRef;
        resourceDefinitionReadyEvent = resourceDefinitionReadyEventRef;
    }

    @Override
    public Serializer get()
    {
        return this;
    }

    @Override
    public byte[] writeEventValue(ResourceDefinitionReadyEventData resourceDefinitionReadyEventData)
    {
        return commonSerializer.headerlessBuilder().resourceDefinitionReadyEvent(
            resourceDefinitionReadyEventData.getReadyCount(),
            resourceDefinitionReadyEventData.getErrorCount()
        ).build();
    }

    @Override
    public LinstorEvent<ResourceDefinitionReadyEventData> getEvent()
    {
        return resourceDefinitionReadyEvent;
    }
}
