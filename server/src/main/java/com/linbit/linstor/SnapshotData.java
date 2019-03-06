package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.linstor.api.pojo.SnapshotPojo;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotDataDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class SnapshotData extends BaseTransactionObject implements Snapshot
{
    // Object identifier
    private final UUID objId;

    // Runtime instance identifier for debug purposes
    private final transient UUID dbgInstanceId;

    private final SnapshotDefinition snapshotDfn;

    // Reference to the node this resource is assigned to
    private final Node node;

    private final SnapshotDataDatabaseDriver dbDriver;

    // DRBD node ID for resource from which this snapshot is derived
    private final NodeId nodeId;

    private final TransactionMap<VolumeNumber, SnapshotVolume> snapshotVlmMap;

    // State flags
    private final StateFlags<SnapshotFlags> flags;

    private final TransactionSimpleObject<SnapshotData, Boolean> deleted;

    // Not persisted because we do not resume snapshot creation after a restart
    private TransactionSimpleObject<SnapshotData, Boolean> suspendResource;

    // Not persisted because we do not resume snapshot creation after a restart
    private TransactionSimpleObject<SnapshotData, Boolean> takeSnapshot;

    private final List<DeviceLayerKind> layerStack;

    public SnapshotData(
        UUID objIdRef,
        SnapshotDefinition snapshotDfnRef,
        Node nodeRef,
        NodeId nodeIdRef,
        long initFlags,
        SnapshotDataDatabaseDriver dbDriverRef,
        TransactionObjectFactory transObjFactory,
        Provider<TransactionMgr> transMgrProviderRef,
        Map<VolumeNumber, SnapshotVolume> snapshotVlmMapRef,
        List<DeviceLayerKind> layerStackRef
    )
    {
        super(transMgrProviderRef);

        objId = objIdRef;
        snapshotDfn = snapshotDfnRef;
        node = nodeRef;
        dbDriver = dbDriverRef;
        nodeId = nodeIdRef;

        dbgInstanceId = UUID.randomUUID();

        snapshotVlmMap = transObjFactory.createTransactionMap(snapshotVlmMapRef, null);

        flags = transObjFactory.createStateFlagsImpl(
            snapshotDfn.getResourceDefinition().getObjProt(),
            this,
            SnapshotFlags.class,
            dbDriverRef.getStateFlagsPersistence(),
            initFlags
        );

        deleted = transObjFactory.createTransactionSimpleObject(this, false, null);

        suspendResource = transObjFactory.createTransactionSimpleObject(this, false, null);
        takeSnapshot = transObjFactory.createTransactionSimpleObject(this, false, null);

        layerStack = Collections.unmodifiableList(new ArrayList<>(layerStackRef));

        transObjs = Arrays.asList(
            snapshotDfn,
            node,
            snapshotVlmMap,
            flags,
            deleted,
            suspendResource,
            takeSnapshot
        );
    }

    @Override
    public UUID getUuid()
    {
        checkDeleted();
        return objId;
    }

    @Override
    public SnapshotDefinition getSnapshotDefinition()
    {
        checkDeleted();
        return snapshotDfn;
    }

    @Override
    public Node getNode()
    {
        checkDeleted();
        return node;
    }

    @Override
    public NodeId getNodeId()
    {
        checkDeleted();
        return nodeId;
    }

    @Override
    public void addSnapshotVolume(AccessContext accCtx, SnapshotVolume snapshotVolume)
        throws AccessDeniedException
    {
        checkDeleted();
        getResourceDefinition().getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        snapshotVlmMap.put(snapshotVolume.getVolumeNumber(), snapshotVolume);
    }

    @Override
    public SnapshotVolume getSnapshotVolume(AccessContext accCtx, VolumeNumber volumeNumber)
        throws AccessDeniedException
    {
        checkDeleted();
        getResourceDefinition().getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return snapshotVlmMap.get(volumeNumber);
    }

    @Override
    public Collection<SnapshotVolume> getAllSnapshotVolumes(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        getResourceDefinition().getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return snapshotVlmMap.values();
    }

    @Override
    public void removeSnapshotVolume(
        AccessContext accCtx,
        SnapshotVolumeData snapshotVolumeData
    )
        throws AccessDeniedException
    {
        checkDeleted();
        getResourceDefinition().getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        snapshotVlmMap.remove(snapshotVolumeData.getVolumeNumber());
    }

    @Override
    public StateFlags<SnapshotFlags> getFlags()
    {
        checkDeleted();
        return flags;
    }

    @Override
    public List<DeviceLayerKind> getLayerStack(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        getResourceDefinition().getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return layerStack;
    }

    @Override
    public void markDeleted(AccessContext accCtx)
        throws AccessDeniedException, SQLException
    {
        getResourceDefinition().getObjProt().requireAccess(accCtx, AccessType.USE);
        getFlags().enableFlags(accCtx, SnapshotFlags.DELETE);
    }

    @Override
    public void delete(AccessContext accCtx)
        throws AccessDeniedException, SQLException
    {
        if (!deleted.get())
        {
            getResourceDefinition().getObjProt().requireAccess(accCtx, AccessType.CONTROL);

            snapshotDfn.removeSnapshot(accCtx, this);
            node.removeSnapshot(this);

            // Shallow copy the volume collection because calling delete results in elements being removed from it
            Collection<SnapshotVolume> snapshotVolumes = new ArrayList<>(snapshotVlmMap.values());
            for (SnapshotVolume snapshotVolume : snapshotVolumes)
            {
                snapshotVolume.delete(accCtx);
            }

            activateTransMgr();
            dbDriver.delete(this);

            deleted.set(true);
        }
    }

    private void checkDeleted()
    {
        if (deleted.get())
        {
            throw new AccessToDeletedDataException("Access to deleted snapshot");
        }
    }

    @Override
    public boolean getSuspendResource(AccessContext accCtx)
        throws AccessDeniedException
    {
        getResourceDefinition().getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return suspendResource.get();
    }

    @Override
    public void setSuspendResource(AccessContext accCtx, boolean suspendResourceRef)
        throws AccessDeniedException
    {
        getResourceDefinition().getObjProt().requireAccess(accCtx, AccessType.CONTROL);
        try
        {
            suspendResource.set(suspendResourceRef);
        }
        catch (SQLException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    @Override
    public boolean getTakeSnapshot(AccessContext accCtx)
        throws AccessDeniedException
    {
        getResourceDefinition().getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return takeSnapshot.get();
    }

    @Override
    public void setTakeSnapshot(AccessContext accCtx, boolean takeSnapshotRef)
        throws AccessDeniedException
    {
        getResourceDefinition().getObjProt().requireAccess(accCtx, AccessType.CONTROL);
        try
        {
            takeSnapshot.set(takeSnapshotRef);
        }
        catch (SQLException exc)
        {
            throw new ImplementationError(exc);
        }
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
    public SnapshotApi getApiData(AccessContext accCtx, Long fullSyncId, Long updateId)
        throws AccessDeniedException
    {
        List<SnapshotVolume.SnapshotVlmApi> snapshotVlms = new ArrayList<>();

        for (SnapshotVolume snapshotVolume : snapshotVlmMap.values())
        {
            snapshotVlms.add(snapshotVolume.getApiData(accCtx));
        }

        return new SnapshotPojo(
            snapshotDfn.getApiData(accCtx),
            objId,
            flags.getFlagsBits(accCtx),
            suspendResource.get(),
            takeSnapshot.get(),
            fullSyncId,
            updateId,
            snapshotVlms
        );
    }
}
