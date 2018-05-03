package com.linbit.linstor.event.handler.protobuf.controller;

import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.event.EventBroker;
import com.linbit.linstor.event.EventIdentifier;
import com.linbit.linstor.event.handler.EventHandler;
import com.linbit.linstor.event.handler.protobuf.ProtobufEventHandler;
import com.linbit.linstor.netcom.Peer;
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
    private final EventBroker eventBroker;
    private final Peer peer;

    @Inject
    public ResourceStateEventHandler(
        EventBroker eventBrokerRef,
        Peer peerRef
    )
    {
        eventBroker = eventBrokerRef;
        peer = peerRef;
    }

    @Override
    public void execute(String eventAction, EventIdentifier eventIdentifier, InputStream eventDataIn)
        throws IOException
    {
        if (eventAction.equals(ApiConsts.EVENT_STREAM_OPEN) || eventAction.equals(ApiConsts.EVENT_STREAM_VALUE))
        {
            EventRscStateOuterClass.EventRscState eventRscState =
                EventRscStateOuterClass.EventRscState.parseDelimitedFrom(eventDataIn);

            peer.getSatelliteState().setOnResource(
                eventIdentifier.getResourceName(),
                SatelliteResourceState::setReady,
                eventRscState.getReady()
            );
        }
        else
        {
            peer.getSatelliteState().unsetOnResource(
                eventIdentifier.getResourceName(),
                SatelliteResourceState::setReady
            );
        }

        eventBroker.forwardEvent(new EventIdentifier(
            ApiConsts.EVENT_RESOURCE_STATE,
            eventIdentifier.getNodeName(),
            eventIdentifier.getResourceName(),
            null
        ), eventAction);
    }
}
