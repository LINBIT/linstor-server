package com.linbit.linstor;

import com.linbit.linstor.api.pojo.SnapshotPojo;
import com.linbit.linstor.security.AccessContext;

import java.util.UUID;

public class SnapshotData implements Snapshot
{
    // Object identifier
    private final UUID objId;

    // Runtime instance identifier for debug purposes
    private final transient UUID dbgInstanceId;

    private final SnapshotDefinition snapshotDfn;

    // Reference to the node this resource is assigned to
    private final Node node;

    private boolean suspendResource;

    private boolean takeSnapshot;

    // Only used on controller
    private boolean resourceSuspended;

    // Only used on controller
    private boolean snapshotTaken;

    public SnapshotData(
        UUID objIdRef,
        SnapshotDefinition snapshotDfnRef,
        Node nodeRef
    )
    {
        objId = objIdRef;
        snapshotDfn = snapshotDfnRef;
        node = nodeRef;

        dbgInstanceId = UUID.randomUUID();
    }

    @Override
    public UUID getUuid()
    {
        return objId;
    }

    @Override
    public SnapshotDefinition getSnapshotDefinition()
    {
        return snapshotDfn;
    }

    @Override
    public Node getNode()
    {
        return node;
    }

    @Override
    public boolean getSuspendResource()
    {
        return suspendResource;
    }

    @Override
    public void setSuspendResource(boolean suspendResourceRef)
    {
        suspendResource = suspendResourceRef;
    }

    @Override
    public boolean getTakeSnapshot()
    {
        return takeSnapshot;
    }

    @Override
    public void setTakeSnapshot(boolean takeSnapshotRef)
    {
        takeSnapshot = takeSnapshotRef;
    }

    @Override
    public boolean isResourceSuspended()
    {
        return resourceSuspended;
    }

    @Override
    public void setResourceSuspended(boolean resourceSuspended)
    {
        this.resourceSuspended = resourceSuspended;
    }

    @Override
    public boolean isSnapshotTaken()
    {
        return snapshotTaken;
    }

    @Override
    public void setSnapshotTaken(boolean snapshotTaken)
    {
        this.snapshotTaken = snapshotTaken;
    }

    @Override
    public String toString()
    {
        return "Node: '" + node.getName() + "', " + snapshotDfn;
    }

    @Override
    public UUID debugGetVolatileUuid()
    {
        return dbgInstanceId;
    }

    @Override
    public SnapshotApi getApiData(AccessContext accCtx)
    {
        return new SnapshotPojo(
            objId,
            snapshotDfn.getName().displayValue,
            snapshotDfn.getUuid(),
            suspendResource,
            takeSnapshot
        );
    }
}
