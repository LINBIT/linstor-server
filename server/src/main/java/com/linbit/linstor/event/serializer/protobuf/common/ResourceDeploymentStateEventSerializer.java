package com.linbit.linstor.event.serializer.protobuf.common;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer;
import com.linbit.linstor.event.LinstorEvent;
import com.linbit.linstor.event.WatchableObject;
import com.linbit.linstor.event.common.ResourceDeploymentStateEvent;
import com.linbit.linstor.event.serializer.EventSerializer;
import com.linbit.linstor.event.serializer.protobuf.ProtobufEventSerializer;

import javax.inject.Inject;
import javax.inject.Singleton;

@ProtobufEventSerializer(
    eventName = ApiConsts.EVENT_RESOURCE_DEPLOYMENT_STATE,
    objectType = WatchableObject.RESOURCE
)
@Singleton
public class ResourceDeploymentStateEventSerializer implements EventSerializer, EventSerializer.Serializer<ApiCallRc>
{
    private final CommonSerializer commonSerializer;
    private final ResourceDeploymentStateEvent resourceDeploymentStateEvent;

    @Inject
    public ResourceDeploymentStateEventSerializer(
        CommonSerializer commonSerializerRef,
        ResourceDeploymentStateEvent resourceDeploymentStateEventRef
    )
    {
        commonSerializer = commonSerializerRef;
        resourceDeploymentStateEvent = resourceDeploymentStateEventRef;
    }

    @Override
    public Serializer get()
    {
        return this;
    }

    @Override
    public byte[] writeEventValue(ApiCallRc apiCallRc)
    {
        return commonSerializer.headerlessBuilder().resourceDeploymentStateEvent(apiCallRc).build();
    }

    @Override
    public LinstorEvent<ApiCallRc> getEvent()
    {
        return resourceDeploymentStateEvent.get();
    }
}
