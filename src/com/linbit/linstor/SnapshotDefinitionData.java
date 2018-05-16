package com.linbit.linstor;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class SnapshotDefinitionData implements SnapshotDefinition
{
    // Object identifier
    private final UUID objId;

    // Runtime instance identifier for debug purposes
    private final transient UUID dbgInstanceId;

    // Reference to the resource definition
    private final ResourceDefinition resourceDfn;

    private final SnapshotName snapshotName;

    private final Map<NodeName, Snapshot> snapshotMap;

    public SnapshotDefinitionData(
        UUID objIdRef,
        ResourceDefinition resourceDfnRef,
        SnapshotName snapshotNameRef
    )
    {
        objId = objIdRef;
        resourceDfn = resourceDfnRef;
        snapshotName = snapshotNameRef;

        dbgInstanceId = UUID.randomUUID();
        snapshotMap = new HashMap<>();
    }

    @Override
    public UUID getUuid()
    {
        return objId;
    }

    @Override
    public ResourceDefinition getResourceDefinition()
    {
        return resourceDfn;
    }

    @Override
    public SnapshotName getName()
    {
        return snapshotName;
    }

    @Override
    public Snapshot getSnapshot(NodeName clNodeName)
    {
        return snapshotMap.get(clNodeName);
    }

    @Override
    public Collection<Snapshot> getAllSnapshots()
    {
        return snapshotMap.values();
    }

    @Override
    public void addSnapshot(Snapshot snapshotRef)
    {
        snapshotMap.put(snapshotRef.getNode().getName(), snapshotRef);
    }

    @Override
    public void removeSnapshot(Snapshot snapshotRef)
    {
        snapshotMap.remove(snapshotRef.getNode().getName());
    }

    @Override
    public String toString()
    {
        return "Rsc: '" + resourceDfn.getName() + "', " +
            "Snapshot: '" + snapshotName + "'";
    }

    @Override
    public UUID debugGetVolatileUuid()
    {
        return dbgInstanceId;
    }
}
