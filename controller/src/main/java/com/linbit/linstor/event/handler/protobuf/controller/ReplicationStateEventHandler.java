package com.linbit.linstor.event.handler.protobuf.controller;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.repository.NodeRepository;
import com.linbit.linstor.event.EventIdentifier;
import com.linbit.linstor.event.common.ReplicationStateEvent;
import com.linbit.linstor.event.handler.EventHandler;
import com.linbit.linstor.event.handler.SatelliteStateHelper;
import com.linbit.linstor.event.handler.protobuf.ProtobufEventHandler;
import com.linbit.linstor.layer.drbd.drbdstate.ReplState;
import com.linbit.linstor.proto.eventdata.EventReplicationStateOuterClass;
import com.linbit.linstor.satellitestate.SatelliteVolumeState;
import com.linbit.utils.PairNonNull;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

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
    private final NodeRepository nodeRepo;

    @Inject
    public ReplicationStateEventHandler(
        SatelliteStateHelper satelliteStateHelperRef,
        ReplicationStateEvent replicationStateEventRef,
        NodeRepository nodeRepositoryRef
    )
    {
        satelliteStateHelper = satelliteStateHelperRef;
        replicationStateEvent = replicationStateEventRef;
        nodeRepo = nodeRepositoryRef;
    }

    /**
     * Returns the mapped nodename from the uname/peer name
     *
     * It is possible that the uname node name isn't yet known, as the first node gets an event containing a node
     * that has not authenticated/connected yet. We ignore such node events for now as they will be published
     * once connected.
     * @return The Linstor node name or null if not found in uname map.
     */
    static @Nullable NodeName getMappedName(NodeRepository nodeRepo, String peerName)
    {
        return nodeRepo.getUname(peerName);
    }

    @Override
    public void execute(String eventAction, EventIdentifier eventIdentifier, InputStream eventDataIn)
        throws IOException
    {
        if (eventAction.equals(InternalApiConsts.EVENT_STREAM_VALUE))
        {
            EventReplicationStateOuterClass.EventReplicationState eventReplicationState =
                EventReplicationStateOuterClass.EventReplicationState.parseDelimitedFrom(eventDataIn);

            @Nullable NodeName mappedName = getMappedName(nodeRepo, eventReplicationState.getPeerName());
            if (mappedName != null)
            {
                PairNonNull<String, ReplState> replicationState = eventReplicationState.getReplicationState().isEmpty() ?
                    new PairNonNull<>(mappedName.displayValue, ReplState.UNKNOWN) :
                    new PairNonNull<>(
                        mappedName.displayValue,
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
                replicationStateEvent.get()
                    .forwardEvent(eventIdentifier.getObjectIdentifier(), eventAction, replicationState);
            }
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
            replicationStateEvent.get()
                .forwardEvent(eventIdentifier.getObjectIdentifier(), eventAction);
        }
    }
}
