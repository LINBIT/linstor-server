package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.linstor.AccessToDeletedDataException;
import com.linbit.linstor.api.pojo.SnapshotDfnListItemPojo;
import com.linbit.linstor.api.pojo.SnapshotDfnPojo;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotDefinitionDataDatabaseDriver;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsAccess;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.propscon.PropsContainerFactory;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;

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

    // Properties container for this snapshot definition
    private final Props snapshotDfnProps;

    // State flags
    private final StateFlags<SnapshotDfnFlags> flags;

    private final TransactionMap<VolumeNumber, SnapshotVolumeDefinition> snapshotVolumeDefinitionMap;

    private final TransactionMap<NodeName, Snapshot> snapshotMap;

    private final TransactionSimpleObject<SnapshotDefinitionData, Boolean> deleted;

    // Not persisted because we do not resume snapshot creation after a restart
    private TransactionSimpleObject<SnapshotDefinitionData, Boolean> inCreation;

    public SnapshotDefinitionData(
        UUID objIdRef,
        ResourceDefinition resourceDfnRef,
        SnapshotName snapshotNameRef,
        long initFlags,
        SnapshotDefinitionDataDatabaseDriver dbDriverRef,
        TransactionObjectFactory transObjFactory,
        PropsContainerFactory propsContainerFactory,
        Provider<? extends TransactionMgr> transMgrProviderRef,
        Map<VolumeNumber, SnapshotVolumeDefinition> snapshotVlmDfnMapRef,
        Map<NodeName, Snapshot> snapshotMapRef
    )
        throws DatabaseException
    {
        super(transMgrProviderRef);
        objId = objIdRef;
        resourceDfn = resourceDfnRef;
        snapshotName = snapshotNameRef;
        dbDriver = dbDriverRef;

        dbgInstanceId = UUID.randomUUID();

        snapshotDfnProps = propsContainerFactory.getInstance(
            PropsContainer.buildPath(resourceDfn.getName(), snapshotName)
        );

        flags = transObjFactory.createStateFlagsImpl(
            resourceDfnRef.getObjProt(),
            this,
            SnapshotDfnFlags.class,
            dbDriverRef.getStateFlagsPersistence(),
            initFlags
        );

        snapshotVolumeDefinitionMap = transObjFactory.createTransactionMap(snapshotVlmDfnMapRef, null);

        snapshotMap = transObjFactory.createTransactionMap(snapshotMapRef, null);

        deleted = transObjFactory.createTransactionSimpleObject(this, false, null);

        inCreation = transObjFactory.createTransactionSimpleObject(this, false, null);

        transObjs = Arrays.asList(
            resourceDfn,
            snapshotVolumeDefinitionMap,
            snapshotMap,
            flags,
            deleted,
            inCreation
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
    public SnapshotVolumeDefinition getSnapshotVolumeDefinition(
        AccessContext accCtx,
        VolumeNumber volumeNumber
    )
        throws AccessDeniedException
    {
        checkDeleted();
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return snapshotVolumeDefinitionMap.get(volumeNumber);
    }

    @Override
    public void addSnapshotVolumeDefinition(
        AccessContext accCtx,
        SnapshotVolumeDefinition snapshotVolumeDefinition
    )
        throws AccessDeniedException
    {
        checkDeleted();
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.USE);
        snapshotVolumeDefinitionMap.put(snapshotVolumeDefinition.getVolumeNumber(), snapshotVolumeDefinition);
    }

    @Override
    public void removeSnapshotVolumeDefinition(
        AccessContext accCtx,
        VolumeNumber volumeNumber
    )
        throws AccessDeniedException
    {
        checkDeleted();
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.USE);
        snapshotVolumeDefinitionMap.remove(volumeNumber);
    }

    @Override
    public Collection<SnapshotVolumeDefinition> getAllSnapshotVolumeDefinitions(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return snapshotVolumeDefinitionMap.values();
    }

    @Override
    public Snapshot getSnapshot(AccessContext accCtx, NodeName clNodeName)
        throws AccessDeniedException
    {
        checkDeleted();
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return snapshotMap.get(clNodeName);
    }

    @Override
    public Collection<Snapshot> getAllSnapshots(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return snapshotMap.values();
    }

    @Override
    public void addSnapshot(AccessContext accCtx, Snapshot snapshotRef)
        throws AccessDeniedException
    {
        checkDeleted();
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.USE);
        snapshotMap.put(snapshotRef.getNodeName(), snapshotRef);
    }

    @Override
    public void removeSnapshot(AccessContext accCtx, Snapshot snapshotRef)
        throws AccessDeniedException
    {
        checkDeleted();
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.USE);
        snapshotMap.remove(snapshotRef.getNodeName());
    }

    @Override
    public Props getProps(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        return PropsAccess.secureGetProps(accCtx, resourceDfn.getObjProt(), snapshotDfnProps);
    }

    @Override
    public StateFlags<SnapshotDfnFlags> getFlags()
    {
        checkDeleted();
        return flags;
    }

    @Override
    public void markDeleted(AccessContext accCtx)
        throws AccessDeniedException, DatabaseException
    {
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.USE);
        getFlags().enableFlags(accCtx, SnapshotDfnFlags.DELETE);
    }

    @Override
    public void delete(AccessContext accCtx)
        throws AccessDeniedException, DatabaseException
    {
        if (!deleted.get())
        {
            resourceDfn.getObjProt().requireAccess(accCtx, AccessType.CONTROL);

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

            snapshotDfnProps.delete();

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
    public boolean getInProgress(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();

        return inCreation.get() ||
            flags.isSet(accCtx, SnapshotDfnFlags.DELETE);
    }

    @Override
    public void setInCreation(AccessContext accCtx, boolean inCreationRef)
        throws DatabaseException, AccessDeniedException
    {
        checkDeleted();
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.CONTROL);
        inCreation.set(inCreationRef);
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
            flags.getFlagsBits(accCtx),
            new TreeMap<>(getProps(accCtx).map())
        );
    }

    @Override
    public SnapshotDfnListItemApi getListItemApiData(AccessContext accCtx)
        throws AccessDeniedException
    {
        return new SnapshotDfnListItemPojo(
            getApiData(accCtx),
            snapshotMap.values().stream()
                .map(Snapshot::getNodeName)
                .map(NodeName::getDisplayName)
                .collect(Collectors.toList())
        );
    }

    @Override
    public String toString()
    {
        return "Rsc: '" + getResourceName() + "', " +
            "Snapshot: '" + snapshotName + "'";
    }

    @Override
    public UUID debugGetVolatileUuid()
    {
        return dbgInstanceId;
    }
}
