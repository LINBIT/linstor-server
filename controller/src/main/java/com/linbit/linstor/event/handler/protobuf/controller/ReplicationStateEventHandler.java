package com.linbit.linstor.event.handler.protobuf.controller;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.event.EventIdentifier;
import com.linbit.linstor.event.common.ReplicationStateEvent;
import com.linbit.linstor.event.handler.EventHandler;
import com.linbit.linstor.event.handler.SatelliteStateHelper;
import com.linbit.linstor.event.handler.protobuf.ProtobufEventHandler;
import com.linbit.linstor.layer.drbd.drbdstate.ReplState;
import com.linbit.linstor.proto.eventdata.EventReplicationStateOuterClass;
import com.linbit.linstor.satellitestate.SatelliteVolumeState;
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;

@ProtobufEventHandler(
    eventName = InternalApiConsts.EVENT_REPLICATION_STATE
)
@Singleton
public class ReplicationStateEventHandler implements EventHandler
{
    private final SatelliteStateHelper satelliteStateHelper;
    private final ReplicationStateEvent replicationStateEvent;

    @Inject
    public ReplicationStateEventHandler(
        SatelliteStateHelper satelliteStateHelperRef,
        ReplicationStateEvent replicationStateEventRef
    )
    {
        satelliteStateHelper = satelliteStateHelperRef;
        replicationStateEvent = replicationStateEventRef;
    }

    @Override
    public void execute(String eventAction, EventIdentifier eventIdentifier, InputStream eventDataIn)
        throws IOException
    {
        Pair<String, ReplState> replicationState;

        if (eventAction.equals(InternalApiConsts.EVENT_STREAM_VALUE))
        {
            EventReplicationStateOuterClass.EventReplicationState eventReplicationState =
                EventReplicationStateOuterClass.EventReplicationState.parseDelimitedFrom(eventDataIn);

            replicationState = eventReplicationState.getReplicationState().isEmpty() ?
                new Pair<>(eventReplicationState.getPeerName(), null) :
                new Pair<>(
                    eventReplicationState.getPeerName(),
                    ReplState.parseReplState(eventReplicationState.getReplicationState()));
            satelliteStateHelper.onSatelliteState(
                eventIdentifier.getNodeName(),
                satelliteState -> satelliteState.setOnVolume(
                    eventIdentifier.getResourceName(),
                    eventIdentifier.getVolumeNumber(),
                    SatelliteVolumeState::setReplicationState,
                    replicationState
                )
            );
        }
        else
        {
            satelliteStateHelper.onSatelliteState(
                eventIdentifier.getNodeName(),
                satelliteState -> satelliteState.unsetOnVolume(
                    eventIdentifier.getResourceName(),
                    eventIdentifier.getVolumeNumber(),
                    SatelliteVolumeState::setReplicationState
                )
            );

            replicationState = null;
        }

        replicationStateEvent.get().forwardEvent(eventIdentifier.getObjectIdentifier(), eventAction, replicationState);
    }
}
