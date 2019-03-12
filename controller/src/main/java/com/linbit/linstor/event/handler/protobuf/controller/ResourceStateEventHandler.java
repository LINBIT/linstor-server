package com.linbit.linstor.event.handler.protobuf.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.event.EventIdentifier;
import com.linbit.linstor.event.common.ResourceStateEvent;
import com.linbit.linstor.event.handler.SatelliteStateHelper;
import com.linbit.linstor.event.common.UsageState;
import com.linbit.linstor.event.handler.EventHandler;
import com.linbit.linstor.event.handler.protobuf.ProtobufEventHandler;
import com.linbit.linstor.proto.eventdata.EventRscStateOuterClass;
import com.linbit.linstor.satellitestate.SatelliteResourceState;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;

@ProtobufEventHandler(
    eventName = ApiConsts.EVENT_RESOURCE_STATE
)
@Singleton
public class ResourceStateEventHandler implements EventHandler
{
    private final SatelliteStateHelper satelliteStateHelper;
    private final ResourceStateEvent resourceStateEvent;

    @Inject
    public ResourceStateEventHandler(
        SatelliteStateHelper satelliteStateHelperRef,
        ResourceStateEvent resourceStateEventRef
    )
    {
        satelliteStateHelper = satelliteStateHelperRef;
        resourceStateEvent = resourceStateEventRef;
    }

    @Override
    public void execute(String eventAction, EventIdentifier eventIdentifier, InputStream eventDataIn)
        throws IOException
    {
        UsageState usageState;

        if (eventAction.equals(ApiConsts.EVENT_STREAM_VALUE))
        {
            EventRscStateOuterClass.EventRscState eventRscState =
                EventRscStateOuterClass.EventRscState.parseDelimitedFrom(eventDataIn);

            Boolean inUse;
            switch (eventRscState.getInUse())
            {
                case FALSE:
                    inUse = false;
                    break;
                case TRUE:
                    inUse = true;
                    break;
                case UNKNOWN:
                    inUse = null;
                    break;
                default:
                    throw new ImplementationError("Unexpected proto InUse enum: " + eventRscState.getInUse());
            }

            satelliteStateHelper.onSatelliteState(
                eventIdentifier.getNodeName(),
                satelliteState -> satelliteState.setOnResource(
                    eventIdentifier.getResourceName(),
                    SatelliteResourceState::setInUse,
                    inUse
                )
            );

            usageState = new UsageState(
                eventRscState.getReady(),
                inUse,
                eventRscState.getUpToDate()
            );
        }
        else
        {
            satelliteStateHelper.onSatelliteState(
                eventIdentifier.getNodeName(),
                satelliteState -> satelliteState.unsetOnResource(
                    eventIdentifier.getResourceName(),
                    SatelliteResourceState::setInUse
                )
            );

            usageState = null;
        }

        resourceStateEvent.get().forwardEvent(eventIdentifier.getObjectIdentifier(), eventAction, usageState);
    }
}
