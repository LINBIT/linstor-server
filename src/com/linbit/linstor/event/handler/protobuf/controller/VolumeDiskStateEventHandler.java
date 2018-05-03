package com.linbit.linstor.event.handler.protobuf.controller;

import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.event.EventBroker;
import com.linbit.linstor.event.EventIdentifier;
import com.linbit.linstor.event.handler.EventHandler;
import com.linbit.linstor.event.handler.protobuf.ProtobufEventHandler;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.eventdata.EventVlmDiskStateOuterClass;
import com.linbit.linstor.satellitestate.SatelliteVolumeState;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;

@ProtobufEventHandler(
    eventName = ApiConsts.EVENT_VOLUME_DISK_STATE
)
public class VolumeDiskStateEventHandler implements EventHandler
{
    private final EventBroker eventBroker;
    private final Peer peer;

    @Inject
    public VolumeDiskStateEventHandler(
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
            EventVlmDiskStateOuterClass.EventVlmDiskState eventVlmDiskState =
                EventVlmDiskStateOuterClass.EventVlmDiskState.parseDelimitedFrom(eventDataIn);

            peer.getSatelliteState().setOnVolume(
                eventIdentifier.getResourceName(),
                eventIdentifier.getVolumeNumber(),
                SatelliteVolumeState::setDiskState,
                eventVlmDiskState.getDiskState()
            );
        }
        else
        {
            peer.getSatelliteState().unsetOnVolume(
                eventIdentifier.getResourceName(),
                eventIdentifier.getVolumeNumber(),
                SatelliteVolumeState::setDiskState
            );
        }

        eventBroker.forwardEvent(new EventIdentifier(
            ApiConsts.EVENT_VOLUME_DISK_STATE,
            eventIdentifier.getNodeName(),
            eventIdentifier.getResourceName(),
            eventIdentifier.getVolumeNumber()
        ), eventAction);
    }
}
