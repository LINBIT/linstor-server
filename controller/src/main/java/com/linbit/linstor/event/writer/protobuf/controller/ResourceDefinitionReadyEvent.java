package com.linbit.linstor.event.writer.protobuf.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiRcUtils;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer;
import com.linbit.linstor.event.EventIdentifier;
import com.linbit.linstor.event.ObjectIdentifier;
import com.linbit.linstor.event.WatchableObject;
import com.linbit.linstor.event.generator.ResourceDeploymentStateGenerator;
import com.linbit.linstor.event.generator.ResourceStateGenerator;
import com.linbit.linstor.event.handler.ResourceDefinitionEventStore;
import com.linbit.linstor.event.writer.EventWriter;
import com.linbit.linstor.event.writer.protobuf.ProtobufEventWriter;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Set;

@ProtobufEventWriter(
    eventName = ApiConsts.EVENT_RESOURCE_DEFINITION_READY,
    objectType = WatchableObject.RESOURCE_DEFINITION
)
@Singleton
public class ResourceDefinitionReadyEvent implements EventWriter
{
    private final CommonSerializer commonSerializer;
    private final ResourceDefinitionEventStore resourceDefinitionEventStore;
    private final ResourceStateGenerator resourceStateGenerator;
    private final ResourceDeploymentStateGenerator resourceDeploymentStateGenerator;

    @Inject
    public ResourceDefinitionReadyEvent(
        CommonSerializer commonSerializerRef,
        ResourceDefinitionEventStore resourceDefinitionEventStoreRef,
        ResourceStateGenerator resourceStateGeneratorRef,
        ResourceDeploymentStateGenerator resourceDeploymentStateGeneratorRef
    )
    {
        commonSerializer = commonSerializerRef;
        resourceDefinitionEventStore = resourceDefinitionEventStoreRef;
        resourceStateGenerator = resourceStateGeneratorRef;
        resourceDeploymentStateGenerator = resourceDeploymentStateGeneratorRef;
    }

    @Override
    public byte[] writeEvent(ObjectIdentifier objectIdentifier)
        throws Exception
    {
        Set<EventIdentifier> eventStreamsForResource =
            resourceDefinitionEventStore.getEventStreamsForResource(objectIdentifier.getResourceName());

        int readyCount = 0;
        int errorCount = 0;
        for (EventIdentifier eventIdentifier : eventStreamsForResource)
        {
            switch (eventIdentifier.getEventName())
            {
                case ApiConsts.EVENT_RESOURCE_STATE:
                    ResourceStateGenerator.UsageState usageState =
                        resourceStateGenerator.generate(eventIdentifier.getObjectIdentifier());
                    if (usageState.getResourceReady() != null && usageState.getResourceReady())
                    {
                        readyCount++;
                    }
                    break;
                case ApiConsts.EVENT_RESOURCE_DEPLOYMENT_STATE:
                    ApiCallRc apiCallRc =
                        resourceDeploymentStateGenerator.generate(eventIdentifier.getObjectIdentifier());
                    if (apiCallRc != null && ApiRcUtils.isError(apiCallRc))
                    {
                        errorCount++;
                    }
                    break;
                default:
                    throw new ImplementationError(
                        "Unknown event " + eventIdentifier.getEventName() + " in resource definition events");
            }
        }

        return commonSerializer.headerlessBuilder().resourceDefinitionReadyEvent(readyCount, errorCount).build();
    }
}
