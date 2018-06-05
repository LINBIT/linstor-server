package com.linbit.linstor.event.writer.protobuf.satellite;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.core.DeploymentStateTracker;
import com.linbit.linstor.core.SnapshotState;
import com.linbit.linstor.event.ObjectIdentifier;
import com.linbit.linstor.event.WatchableObject;
import com.linbit.linstor.event.writer.EventWriter;
import com.linbit.linstor.event.writer.protobuf.ProtobufEventWriter;

import javax.inject.Inject;
import javax.inject.Singleton;

@ProtobufEventWriter(
    eventName = InternalApiConsts.EVENT_IN_PROGRESS_SNAPSHOT,
    objectType = WatchableObject.SNAPSHOT
)
@Singleton
public class InProgressSnapshotEvent implements EventWriter
{
    private final DeploymentStateTracker deploymentStateTracker;
    private final CtrlStltSerializer ctrlStltSerializer;

    @Inject
    public InProgressSnapshotEvent(
        DeploymentStateTracker deploymentStateTrackerRef,
        CtrlStltSerializer ctrlStltSerializerRef
    )
    {
        deploymentStateTracker = deploymentStateTrackerRef;
        ctrlStltSerializer = ctrlStltSerializerRef;
    }

    @Override
    public byte[] writeEvent(ObjectIdentifier objectIdentifier)
        throws Exception
    {
        SnapshotState snapshotState =
            deploymentStateTracker.getSnapshotStates(objectIdentifier.getResourceName()).stream()
                .filter(state -> state.getSnapshotName().equals(objectIdentifier.getSnapshotName()))
                .findAny()
                .orElse(null);

        return snapshotState == null ?
            null :
            ctrlStltSerializer.builder().inProgressSnapshotEvent(snapshotState).build();
    }

    @Override
    public void clear(ObjectIdentifier objectIdentifier)
        throws Exception
    {
        deploymentStateTracker.removeSnapshotStates(objectIdentifier.getResourceName());
    }
}
