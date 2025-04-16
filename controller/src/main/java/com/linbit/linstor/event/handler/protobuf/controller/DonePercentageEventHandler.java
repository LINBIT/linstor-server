package com.linbit.linstor.event.handler.protobuf.controller;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.repository.NodeRepository;
import com.linbit.linstor.event.EventIdentifier;
import com.linbit.linstor.event.common.DonePercentageEvent;
import com.linbit.linstor.event.handler.EventHandler;
import com.linbit.linstor.event.handler.SatelliteStateHelper;
import com.linbit.linstor.event.handler.protobuf.ProtobufEventHandler;
import com.linbit.linstor.proto.eventdata.EventDonePercentageOuterClass;
import com.linbit.linstor.satellitestate.SatelliteVolumeState;
import com.linbit.linstor.security.AccessContext;
import com.linbit.utils.Pair;

import static com.linbit.linstor.event.handler.protobuf.controller.ReplicationStateEventHandler.getMappedName;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

@ProtobufEventHandler(
    eventName = InternalApiConsts.EVENT_DONE_PERCENTAGE
)
@Singleton
public class DonePercentageEventHandler implements EventHandler
{
    private final SatelliteStateHelper satelliteStateHelper;
    private final DonePercentageEvent donePercentageEvent;
    private final AccessContext sysCtx;
    private final NodeRepository nodeRepo;

    @Inject
    public DonePercentageEventHandler(
        SatelliteStateHelper satelliteStateHelperRef,
        DonePercentageEvent donePercentageEventRef,
        @SystemContext AccessContext sysCtxRef,
        NodeRepository nodeRepositoryRef
    )
    {
        satelliteStateHelper = satelliteStateHelperRef;
        donePercentageEvent = donePercentageEventRef;
        sysCtx = sysCtxRef;
        nodeRepo = nodeRepositoryRef;
    }

    @Override
    public void execute(String eventAction, EventIdentifier eventIdentifier, InputStream eventDataIn)
        throws IOException
    {
        Pair<String, Optional<Float>> donePercentage;

        if (eventAction.equals(InternalApiConsts.EVENT_STREAM_VALUE))
        {
            EventDonePercentageOuterClass.EventDonePercentage eventDonePercentage =
                EventDonePercentageOuterClass.EventDonePercentage.parseDelimitedFrom(eventDataIn);

            NodeName mappedName = getMappedName(nodeRepo, sysCtx, eventDonePercentage.getPeerName());
            donePercentage = eventDonePercentage.hasDonePercentage() ?
                new Pair<>(mappedName.displayValue, Optional.of(eventDonePercentage.getDonePercentage())) :
                new Pair<>(mappedName.displayValue, Optional.empty());
            satelliteStateHelper.onSatelliteState(
                eventIdentifier.getNodeName(),
                satelliteState -> satelliteState.setOnVolume(
                    eventIdentifier.getResourceName(),
                    eventIdentifier.getVolumeNumber(),
                    SatelliteVolumeState::setDonePercentage,
                    donePercentage
                )
            );
        }
        else
        {
            donePercentage = null;
            satelliteStateHelper.onSatelliteState(
                eventIdentifier.getNodeName(),
                satelliteState -> satelliteState.unsetOnVolume(
                    eventIdentifier.getResourceName(),
                    eventIdentifier.getVolumeNumber(),
                    SatelliteVolumeState::setDonePercentage
                )
            );
        }

        donePercentageEvent.get().forwardEvent(
            eventIdentifier.getObjectIdentifier(), eventAction, donePercentage);
    }
}
