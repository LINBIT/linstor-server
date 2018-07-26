package com.linbit.linstor.event.handler;

import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.event.EventBroker;
import com.linbit.linstor.event.EventIdentifier;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class ResourceDefinitionEventStreamTracker
{
    private final ResourceDefinitionEventStore resourceDefinitionEventStore;
    private final EventBroker eventBroker;

    @Inject
    public ResourceDefinitionEventStreamTracker(
        ResourceDefinitionEventStore resourceDefinitionEventStoreRef,
        EventBroker eventBrokerRef
    )
    {
        resourceDefinitionEventStore = resourceDefinitionEventStoreRef;
        eventBroker = eventBrokerRef;
    }

    public void resourceEventReceived(EventIdentifier eventIdentifier, String eventStreamAction)
    {
        resourceDefinitionEventStore.getLock().lock();
        try
        {
            String resourceDefinitionAction = ApiConsts.EVENT_STREAM_VALUE;

            if (eventStreamAction.equals(ApiConsts.EVENT_STREAM_OPEN))
            {
                if (!resourceDefinitionEventStore.contains(eventIdentifier.getResourceName()))
                {
                    resourceDefinitionAction = ApiConsts.EVENT_STREAM_OPEN;
                }

                resourceDefinitionEventStore.add(eventIdentifier);
            }
            else if (eventStreamAction.equals(ApiConsts.EVENT_STREAM_CLOSE_REMOVED) ||
                eventStreamAction.equals(ApiConsts.EVENT_STREAM_CLOSE_NO_CONNECTION))
            {
                resourceDefinitionEventStore.remove(eventIdentifier);

                if (!resourceDefinitionEventStore.contains(eventIdentifier.getResourceName()))
                {
                    resourceDefinitionAction = ApiConsts.EVENT_STREAM_CLOSE_REMOVED;
                }
            }

            eventBroker.forwardEvent(
                EventIdentifier.resourceDefinition(
                    ApiConsts.EVENT_RESOURCE_DEFINITION_READY, eventIdentifier.getResourceName()),
                resourceDefinitionAction
            );
        }
        finally
        {
            resourceDefinitionEventStore.getLock().unlock();
        }
    }
}
