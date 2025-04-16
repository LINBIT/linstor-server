package com.linbit.linstor.event.handler.protobuf.controller;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.SystemContext;
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
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.utils.Pair;

import javax.annotation.Nullable;
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
    private final AccessContext sysCtx;
    private final NodeRepository nodeRepo;

    @Inject
    public ReplicationStateEventHandler(
        SatelliteStateHelper satelliteStateHelperRef,
        ReplicationStateEvent replicationStateEventRef,
        @SystemContext AccessContext sysCtxRef,
        NodeRepository nodeRepositoryRef
    )
    {
        satelliteStateHelper = satelliteStateHelperRef;
        replicationStateEvent = replicationStateEventRef;
        sysCtx = sysCtxRef;
        nodeRepo = nodeRepositoryRef;
    }

    static NodeName getMappedName(NodeRepository nodeRepo, AccessContext sysCtx, String peerName)
    {
        NodeName ret;
        try
        {
            @Nullable NodeName mappedName = nodeRepo.getUname(sysCtx, peerName);
            if (mappedName == null)
            {
                ret = LinstorParsingUtils.asNodeName("notfound." + peerName);
            }
            else
            {
                ret = mappedName;
            }
        }
        catch (AccessDeniedException ignored)
        {
            ret = LinstorParsingUtils.asNodeName("accden." + peerName);
        }
        return ret;
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

            NodeName mappedName = getMappedName(nodeRepo, sysCtx, eventReplicationState.getPeerName());
            replicationState = eventReplicationState.getReplicationState().isEmpty() ?
                new Pair<>(mappedName.displayValue, null) :
                new Pair<>(
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
