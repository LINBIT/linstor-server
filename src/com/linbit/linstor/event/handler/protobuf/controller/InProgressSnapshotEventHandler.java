package com.linbit.linstor.event.handler.protobuf.controller;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.SnapshotName;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.SnapshotState;
import com.linbit.linstor.event.EventIdentifier;
import com.linbit.linstor.event.generator.SatelliteStateHelper;
import com.linbit.linstor.event.handler.EventHandler;
import com.linbit.linstor.event.handler.SnapshotStateMachine;
import com.linbit.linstor.event.handler.protobuf.ProtobufEventHandler;
import com.linbit.linstor.proto.javainternal.EventInProgressSnapshotOuterClass;
import com.linbit.linstor.satellitestate.SatelliteResourceState;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

@ProtobufEventHandler(
    eventName = InternalApiConsts.EVENT_IN_PROGRESS_SNAPSHOT
)
public class InProgressSnapshotEventHandler implements EventHandler
{
    private final SatelliteStateHelper satelliteStateHelper;
    private final SnapshotStateMachine snapshotStateMachine;

    @Inject
    public InProgressSnapshotEventHandler(
        SatelliteStateHelper satelliteStateHelperRef,
        SnapshotStateMachine snapshotStateMachineRef
    )
    {
        satelliteStateHelper = satelliteStateHelperRef;
        snapshotStateMachine = snapshotStateMachineRef;
    }

    @Override
    public void execute(String eventAction, EventIdentifier eventIdentifier, InputStream eventDataIn)
        throws IOException
    {
        try
        {
            if (eventAction.equals(ApiConsts.EVENT_STREAM_OPEN) ||
                eventAction.equals(ApiConsts.EVENT_STREAM_VALUE))
            {
                List<SnapshotState> snapshotStates = new ArrayList<>();

                EventInProgressSnapshotOuterClass.EventInProgressSnapshot eventInProgressSnapshot =
                    EventInProgressSnapshotOuterClass.EventInProgressSnapshot.parseDelimitedFrom(eventDataIn);

                for (EventInProgressSnapshotOuterClass.InProgressSnapshot inProgressSnapshot :
                    eventInProgressSnapshot.getSnapshotsList())
                {
                    snapshotStates.add(new SnapshotState(
                        new SnapshotName(inProgressSnapshot.getSnapshotName()),
                        inProgressSnapshot.getSuspended(),
                        inProgressSnapshot.getSnapshotTaken()
                    ));
                }

                satelliteStateHelper.onSatelliteState(
                    eventIdentifier.getNodeName(),
                    satelliteState -> satelliteState.setOnResource(
                        eventIdentifier.getResourceName(),
                        SatelliteResourceState::setSnapshotStates,
                        snapshotStates
                    )
                );
            }
            else
            {
                satelliteStateHelper.onSatelliteState(
                    eventIdentifier.getNodeName(),
                    satelliteState -> satelliteState.unsetOnResource(
                        eventIdentifier.getResourceName(),
                        SatelliteResourceState::setSnapshotStates
                    )
                );
            }
        }
        catch (InvalidNameException exc)
        {
            throw new ImplementationError("Received invalid InProgressSnapshot event", exc);
        }

        snapshotStateMachine.stepResourceSnapshots(eventIdentifier, false);
    }
}
