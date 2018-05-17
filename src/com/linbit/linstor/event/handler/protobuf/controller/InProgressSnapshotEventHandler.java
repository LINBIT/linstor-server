package com.linbit.linstor.event.handler.protobuf.controller;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.SnapshotState;
import com.linbit.linstor.event.EventIdentifier;
import com.linbit.linstor.event.generator.SatelliteStateHelper;
import com.linbit.linstor.event.handler.EventHandler;
import com.linbit.linstor.event.handler.SnapshotStateMachine;
import com.linbit.linstor.event.handler.protobuf.ProtobufEventHandler;
import com.linbit.linstor.proto.javainternal.EventInProgressSnapshotOuterClass;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;

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
        if (eventAction.equals(ApiConsts.EVENT_STREAM_OPEN) ||
            eventAction.equals(ApiConsts.EVENT_STREAM_VALUE))
        {
            EventInProgressSnapshotOuterClass.EventInProgressSnapshot inProgressSnapshot =
                EventInProgressSnapshotOuterClass.EventInProgressSnapshot.parseDelimitedFrom(eventDataIn);

            SnapshotState snapshotState = new SnapshotState(
                eventIdentifier.getSnapshotName(),
                inProgressSnapshot.getSuspended(),
                inProgressSnapshot.getSnapshotTaken()
            );

            satelliteStateHelper.onSatelliteState(
                eventIdentifier.getNodeName(),
                satelliteState -> satelliteState.setSnapshotState(
                    eventIdentifier.getResourceName(),
                    eventIdentifier.getSnapshotName(),
                    snapshotState
                )
            );
        }
        else
        {
            satelliteStateHelper.onSatelliteState(
                eventIdentifier.getNodeName(),
                satelliteState -> satelliteState.setSnapshotState(
                    eventIdentifier.getResourceName(),
                    eventIdentifier.getSnapshotName(),
                    null
                )
            );
        }

        snapshotStateMachine.stepResourceSnapshots(eventIdentifier, false);
    }
}
