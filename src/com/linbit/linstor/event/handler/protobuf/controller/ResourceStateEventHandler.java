package com.linbit.linstor.event.handler.protobuf.controller;

import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.event.EventBroker;
import com.linbit.linstor.event.EventIdentifier;
import com.linbit.linstor.event.generator.SatelliteStateHelper;
import com.linbit.linstor.event.handler.EventHandler;
import com.linbit.linstor.event.handler.ResourceDefinitionEventStreamTracker;
import com.linbit.linstor.event.handler.protobuf.ProtobufEventHandler;
import com.linbit.linstor.proto.eventdata.EventRscStateOuterClass;
import com.linbit.linstor.satellitestate.SatelliteResourceState;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;

@ProtobufEventHandler(
    eventName = ApiConsts.EVENT_RESOURCE_STATE
)
public class ResourceStateEventHandler implements EventHandler
{
    private final SatelliteStateHelper satelliteStateHelper;
    private final EventBroker eventBroker;
    private final ResourceDefinitionEventStreamTracker resourceDefinitionEventStreamTracker;

    @Inject
    public ResourceStateEventHandler(
        SatelliteStateHelper satelliteStateHelperRef, EventBroker eventBrokerRef,
        ResourceDefinitionEventStreamTracker resourceDefinitionEventStreamTrackerRef
    )
    {
        satelliteStateHelper = satelliteStateHelperRef;
        eventBroker = eventBrokerRef;
        resourceDefinitionEventStreamTracker = resourceDefinitionEventStreamTrackerRef;
    }

    @Override
    public void execute(String eventAction, EventIdentifier eventIdentifier, InputStream eventDataIn)
        throws IOException
    {
        if (eventAction.equals(ApiConsts.EVENT_STREAM_OPEN) || eventAction.equals(ApiConsts.EVENT_STREAM_VALUE))
        {
            EventRscStateOuterClass.EventRscState eventRscState =
                EventRscStateOuterClass.EventRscState.parseDelimitedFrom(eventDataIn);

            satelliteStateHelper.onSatelliteState(
                eventIdentifier.getNodeName(),
                satelliteState -> satelliteState.setOnResource(
                    eventIdentifier.getResourceName(),
                    SatelliteResourceState::setReady,
                    eventRscState.getReady()
                )
            );
        }
        else
        {
            satelliteStateHelper.onSatelliteState(
                eventIdentifier.getNodeName(),
                satelliteState -> satelliteState.unsetOnResource(
                    eventIdentifier.getResourceName(),
                    SatelliteResourceState::setReady
                )
            );
        }

        eventBroker.forwardEvent(eventIdentifier, eventAction);

        resourceDefinitionEventStreamTracker.resourceEventReceived(eventIdentifier, eventAction);
    }
}
