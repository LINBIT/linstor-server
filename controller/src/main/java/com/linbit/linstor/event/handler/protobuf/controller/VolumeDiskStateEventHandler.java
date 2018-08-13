package com.linbit.linstor.event.handler.protobuf.controller;

import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.event.EventIdentifier;
import com.linbit.linstor.event.common.VolumeDiskStateEvent;
import com.linbit.linstor.event.handler.SatelliteStateHelper;
import com.linbit.linstor.event.handler.EventHandler;
import com.linbit.linstor.event.handler.protobuf.ProtobufEventHandler;
import com.linbit.linstor.proto.eventdata.EventVlmDiskStateOuterClass;
import com.linbit.linstor.satellitestate.SatelliteVolumeState;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;

@ProtobufEventHandler(
    eventName = ApiConsts.EVENT_VOLUME_DISK_STATE
)
@Singleton
public class VolumeDiskStateEventHandler implements EventHandler
{
    private final SatelliteStateHelper satelliteStateHelper;
    private final VolumeDiskStateEvent volumeDiskStateEvent;

    @Inject
    public VolumeDiskStateEventHandler(
        SatelliteStateHelper satelliteStateHelperRef,
        VolumeDiskStateEvent volumeDiskStateEventRef
    )
    {
        satelliteStateHelper = satelliteStateHelperRef;
        volumeDiskStateEvent = volumeDiskStateEventRef;
    }

    @Override
    public void execute(String eventAction, EventIdentifier eventIdentifier, InputStream eventDataIn)
        throws IOException
    {
        String diskState;

        if (eventAction.equals(ApiConsts.EVENT_STREAM_VALUE))
        {
            EventVlmDiskStateOuterClass.EventVlmDiskState eventVlmDiskState =
                EventVlmDiskStateOuterClass.EventVlmDiskState.parseDelimitedFrom(eventDataIn);

            satelliteStateHelper.onSatelliteState(
                eventIdentifier.getNodeName(),
                satelliteState -> satelliteState.setOnVolume(
                    eventIdentifier.getResourceName(),
                    eventIdentifier.getVolumeNumber(),
                    SatelliteVolumeState::setDiskState,
                    eventVlmDiskState.getDiskState()
                )
            );

            diskState = eventVlmDiskState.getDiskState();
        }
        else
        {
            satelliteStateHelper.onSatelliteState(
                eventIdentifier.getNodeName(),
                satelliteState -> satelliteState.unsetOnVolume(
                    eventIdentifier.getResourceName(),
                    eventIdentifier.getVolumeNumber(),
                    SatelliteVolumeState::setDiskState
                )
            );

            diskState = null;
        }

        volumeDiskStateEvent.get().forwardEvent(eventIdentifier.getObjectIdentifier(), eventAction, diskState);
    }
}
