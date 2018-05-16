package com.linbit.linstor.event.writer.protobuf.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
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
                    Boolean ready = resourceStateGenerator.generate(eventIdentifier.getObjectIdentifier());
                    if (ready != null && ready)
                    {
                        readyCount++;
                    }
                    break;
                case ApiConsts.EVENT_RESOURCE_DEPLOYMENT_STATE:
                    ApiCallRc apiCallRc =
                        resourceDeploymentStateGenerator.generate(eventIdentifier.getObjectIdentifier());
                    if (apiCallRc != null && isError(apiCallRc))
                    {
                        errorCount++;
                    }
                    break;
                default:
                    throw new ImplementationError(
                        "Unknown event " + eventIdentifier.getEventName() + " in resource definition events");
            }
        }

        return commonSerializer.builder().resourceDefinitionReadyEvent(readyCount, errorCount).build();
    }

    private boolean isError(ApiCallRc apiCallRc)
    {
        return apiCallRc.getEntries().stream().anyMatch(this::entryIsError);
    }

    private boolean entryIsError(ApiCallRc.RcEntry rcEntry)
    {
        return
            (rcEntry.getReturnCode() & ApiConsts.MASK_ERROR) == ApiConsts.MASK_ERROR ||
            (rcEntry.getReturnCode() & ApiConsts.MASK_WARN) == ApiConsts.MASK_WARN;
    }
}
