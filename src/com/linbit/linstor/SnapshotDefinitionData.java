package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.linstor.api.pojo.SnapshotDfnPojo;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotDefinitionDataDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMap;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;

import javax.inject.Provider;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
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

    private final SnapshotDefinitionDataDatabaseDriver dbDriver;

    // State flags
    private final StateFlags<SnapshotDfnFlags> flags;

    private final TransactionMap<VolumeNumber, SnapshotVolumeDefinition> snapshotVolumeDefinitionMap;

    private final TransactionMap<NodeName, Snapshot> snapshotMap;

    private final TransactionSimpleObject<SnapshotDefinitionData, Boolean> deleted;

    public SnapshotDefinitionData(
        UUID objIdRef,
        ResourceDefinition resourceDfnRef,
        SnapshotName snapshotNameRef,
        long initFlags,
        SnapshotDefinitionDataDatabaseDriver dbDriverRef,
        TransactionObjectFactory transObjFactory,
        Provider<TransactionMgr> transMgrProviderRef,
        Map<VolumeNumber, SnapshotVolumeDefinition> snapshotVlmDfnMapRef,
        Map<NodeName, Snapshot> snapshotMapRef
    )
    {
        super(transMgrProviderRef);
        objId = objIdRef;
        resourceDfn = resourceDfnRef;
        snapshotName = snapshotNameRef;
        dbDriver = dbDriverRef;

        dbgInstanceId = UUID.randomUUID();

        flags = transObjFactory.createStateFlagsImpl(
            resourceDfnRef.getObjProt(),
            this,
            SnapshotDfnFlags.class,
            dbDriverRef.getStateFlagsPersistence(),
            initFlags
        );

        snapshotVolumeDefinitionMap = transObjFactory.createTransactionMap(snapshotVlmDfnMapRef, null);

        snapshotMap = transObjFactory.createTransactionMap(snapshotMapRef, null);;

        deleted = transObjFactory.createTransactionSimpleObject(this, false, null);

        transObjs = Arrays.asList(
            resourceDfn,
            snapshotVolumeDefinitionMap,
            snapshotMap,
            flags,
            deleted
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
        checkDeleted();
        return resourceDfn;
    }

    @Override
    public SnapshotName getName()
    {
        checkDeleted();
        return snapshotName;
    }

    @Override
    public SnapshotVolumeDefinition getSnapshotVolumeDefinition(VolumeNumber volumeNumber)
    {
        checkDeleted();
        return snapshotVolumeDefinitionMap.get(volumeNumber);
    }

    @Override
    public void addSnapshotVolumeDefinition(SnapshotVolumeDefinition snapshotVolumeDefinition)
    {
        checkDeleted();
        snapshotVolumeDefinitionMap.put(snapshotVolumeDefinition.getVolumeNumber(), snapshotVolumeDefinition);
    }

    @Override
    public void removeSnapshotVolumeDefinition(VolumeNumber volumeNumber)
    {
        checkDeleted();
        snapshotVolumeDefinitionMap.remove(volumeNumber);
    }

    @Override
    public Collection<SnapshotVolumeDefinition> getAllSnapshotVolumeDefinitions()
    {
        checkDeleted();
        return snapshotVolumeDefinitionMap.values();
    }

    @Override
    public Snapshot getSnapshot(NodeName clNodeName)
    {
        checkDeleted();
        return snapshotMap.get(clNodeName);
    }

    @Override
    public Collection<Snapshot> getAllSnapshots()
    {
        checkDeleted();
        return snapshotMap.values();
    }

    @Override
    public void addSnapshot(Snapshot snapshotRef)
    {
        checkDeleted();
        snapshotMap.put(snapshotRef.getNode().getName(), snapshotRef);
    }

    @Override
    public void removeSnapshot(Snapshot snapshotRef)
    {
        checkDeleted();
        snapshotMap.remove(snapshotRef.getNode().getName());
    }

    @Override
    public StateFlags<SnapshotDfnFlags> getFlags()
    {
        checkDeleted();
        return flags;
    }

    @Override
    public void markDeleted(AccessContext accCtx)
        throws AccessDeniedException, SQLException
    {
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.USE);
        getFlags().enableFlags(accCtx, SnapshotDfnFlags.DELETE);
    }

    @Override
    public void delete(AccessContext accCtx)
        throws AccessDeniedException, SQLException
    {
        if (!deleted.get())
        {
            if (!snapshotMap.isEmpty())
            {
                throw new ImplementationError("Cannot delete snapshot definition which contains snapshots");
            }

            resourceDfn.removeSnapshotDfn(accCtx, snapshotName);

            // Shallow copy the volume collection because calling delete results in elements being removed from it
            Collection<SnapshotVolumeDefinition> snapshotVolumeDefinitions =
                new ArrayList<>(snapshotVolumeDefinitionMap.values());
            for (SnapshotVolumeDefinition snapshotVolumeDefinition : snapshotVolumeDefinitions)
            {
                snapshotVolumeDefinition.delete(accCtx);
            }

            activateTransMgr();
            dbDriver.delete(this);

            deleted.set(true);
        }
    }

    @Override
    public boolean isDeleted()
    {
        return deleted.get();
    }

    private void checkDeleted()
    {
        if (deleted.get())
        {
            throw new AccessToDeletedDataException("Access to deleted snapshot definition");
        }
    }

    @Override
    public SnapshotDfnApi getApiData(AccessContext accCtx)
        throws AccessDeniedException
    {
        List<SnapshotVolumeDefinition.SnapshotVlmDfnApi> snapshotVlmDfns = new ArrayList<>();

        for (SnapshotVolumeDefinition snapshotVolumeDefinition : snapshotVolumeDefinitionMap.values())
        {
            snapshotVlmDfns.add(snapshotVolumeDefinition.getApiData(accCtx));
        }

        return new SnapshotDfnPojo(
            resourceDfn.getApiData(accCtx),
            objId,
            snapshotName.getDisplayName(),
            snapshotVlmDfns,
            flags.getFlagsBits(accCtx)
        );
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
