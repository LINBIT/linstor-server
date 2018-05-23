package com.linbit.linstor;

import com.linbit.linstor.dbdrivers.interfaces.SnapshotDefinitionDataDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Provider;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;

public class SnapshotDefinitionData extends BaseTransactionObject implements SnapshotDefinition
{
    // Object identifier
    private final UUID objId;

    // Runtime instance identifier for debug purposes
    private final transient UUID dbgInstanceId;

    // Reference to the resource definition
    private final ResourceDefinition resourceDfn;

    private final SnapshotName snapshotName;

    // State flags
    private final StateFlags<SnapshotDfnFlags> flags;

    private final Map<NodeName, Snapshot> snapshotMap;

    public SnapshotDefinitionData(
        UUID objIdRef,
        ResourceDefinition resourceDfnRef,
        SnapshotName snapshotNameRef,
        long initFlags,
        SnapshotDefinitionDataDatabaseDriver dbDriverRef,
        TransactionObjectFactory transObjFactory,
        Provider<TransactionMgr> transMgrProviderRef,
        Map<NodeName, Snapshot> snapshotMapRef
    )
    {
        super(transMgrProviderRef);
        objId = objIdRef;
        resourceDfn = resourceDfnRef;
        snapshotName = snapshotNameRef;

        dbgInstanceId = UUID.randomUUID();
        snapshotMap = snapshotMapRef;

        flags = transObjFactory.createStateFlagsImpl(
            resourceDfnRef.getObjProt(),
            this,
            SnapshotDfnFlags.class,
            dbDriverRef.getStateFlagsPersistence(),
            initFlags
        );

        transObjs = Arrays.asList(
            resourceDfn,
            flags
        );
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
    public StateFlags<SnapshotDfnFlags> getFlags()
    {
        return flags;
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
