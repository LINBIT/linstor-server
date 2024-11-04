package com.linbit.linstor.event.handler.protobuf.controller;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.event.EventIdentifier;
import com.linbit.linstor.event.common.ConnectionStateEvent;
import com.linbit.linstor.event.handler.EventHandler;
import com.linbit.linstor.event.handler.SatelliteStateHelper;
import com.linbit.linstor.event.handler.protobuf.ProtobufEventHandler;
import com.linbit.linstor.proto.eventdata.EventConnStateOuterClass;

import javax.inject.Inject;

import java.io.IOException;
import java.io.InputStream;

@ProtobufEventHandler(
    eventName = InternalApiConsts.EVENT_CONNECTION_STATE
)

public class ConnectionStateEventHandler implements EventHandler
{
    private final SatelliteStateHelper satelliteStateHelper;
    private final ConnectionStateEvent connectionStateEvent;

    @Inject
    public ConnectionStateEventHandler(
        SatelliteStateHelper satelliteStateHelperRef,
        ConnectionStateEvent connectionStateEventRef
    )
    {
        satelliteStateHelper = satelliteStateHelperRef;
        connectionStateEvent = connectionStateEventRef;
    }

    @Override
    public void execute(String eventAction, EventIdentifier eventIdentifier, InputStream eventDataIn)
        throws IOException
    {
        if (eventAction.equals(InternalApiConsts.EVENT_STREAM_VALUE))
        {
            EventConnStateOuterClass.EventConnState eventVlmDiskState =
                EventConnStateOuterClass.EventConnState.parseDelimitedFrom(eventDataIn);

            satelliteStateHelper.onSatelliteState(
                eventIdentifier.getNodeName(),
                satelliteState -> satelliteState.setOnConnection(
                    eventIdentifier.getResourceName(),
                    eventIdentifier.getNodeName(),
                    eventIdentifier.getPeerNodeName(),
                    eventVlmDiskState.getConnectionState()
                )
            );

            connectionStateEvent.get()
                .forwardEvent(
                    eventIdentifier.getObjectIdentifier(),
                    eventAction,
                    eventVlmDiskState.getConnectionState()
                );
        }
        else
        {
            satelliteStateHelper.onSatelliteState(
                eventIdentifier.getNodeName(),
                satelliteState -> satelliteState.unsetOnConnection(
                    eventIdentifier.getResourceName(),
                    eventIdentifier.getNodeName(),
                    eventIdentifier.getPeerNodeName()
                )
            );

            connectionStateEvent.get().forwardEvent(eventIdentifier.getObjectIdentifier(), eventAction);
        }
    }
}
