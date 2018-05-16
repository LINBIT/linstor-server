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
import java.util.List;

@ProtobufEventWriter(
    eventName = InternalApiConsts.EVENT_IN_PROGRESS_SNAPSHOT,
    objectType = WatchableObject.RESOURCE
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

    public byte[] writeEvent(ObjectIdentifier objectIdentifier)
        throws Exception
    {
        List<SnapshotState> snapshotStates =
            deploymentStateTracker.getSnapshotStates(objectIdentifier.getResourceName());

        return snapshotStates == null ?
            null :
            ctrlStltSerializer.builder().inProgressSnapshotEvent(snapshotStates).build();
    }
}
