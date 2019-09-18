package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.linstor.AccessToDeletedDataException;
import com.linbit.linstor.DbgInstanceUuid;
import com.linbit.linstor.api.pojo.SnapshotPojo;
import com.linbit.linstor.core.apis.SnapshotApi;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.types.NodeId;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.stateflags.Flags;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMap;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;

import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class Snapshot extends BaseTransactionObject implements DbgInstanceUuid, Comparable<Snapshot>
{
    public static interface InitMaps
    {
        Map<VolumeNumber, SnapshotVolume> getSnapshotVlmMap();
    }

    // Object identifier
    private final UUID objId;

    // Runtime instance identifier for debug purposes
    private final transient UUID dbgInstanceId;

    private final SnapshotDefinition snapshotDfn;

    // Reference to the node this resource is assigned to
    private final Node node;

    private final SnapshotDatabaseDriver dbDriver;

    // DRBD node ID for resource from which this snapshot is derived
    private final NodeId nodeId;

    private final TransactionMap<VolumeNumber, SnapshotVolume> snapshotVlmMap;

    // State flags
    private final StateFlags<Flags> flags;

    private final TransactionSimpleObject<Snapshot, Boolean> deleted;

    // Not persisted because we do not resume snapshot creation after a restart
    private TransactionSimpleObject<Snapshot, Boolean> suspendResource;

    // Not persisted because we do not resume snapshot creation after a restart
    private TransactionSimpleObject<Snapshot, Boolean> takeSnapshot;

    private final List<DeviceLayerKind> layerStack;

    public Snapshot(
        UUID objIdRef,
        SnapshotDefinition snapshotDfnRef,
        Node nodeRef,
        NodeId nodeIdRef,
        long initFlags,
        SnapshotDatabaseDriver dbDriverRef,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProviderRef,
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
            Flags.class,
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

    public UUID getUuid()
    {
        checkDeleted();
        return objId;
    }

    public SnapshotDefinition getSnapshotDefinition()
    {
        checkDeleted();
        return snapshotDfn;
    }

    public Node getNode()
    {
        checkDeleted();
        return node;
    }

    public NodeId getNodeId()
    {
        checkDeleted();
        return nodeId;
    }

    public void addSnapshotVolume(AccessContext accCtx, SnapshotVolume snapshotVolume)
        throws AccessDeniedException
    {
        checkDeleted();
        requireAccess(accCtx, AccessType.CHANGE);
        snapshotVlmMap.put(snapshotVolume.getVolumeNumber(), snapshotVolume);
    }

    public SnapshotVolume getSnapshotVolume(AccessContext accCtx, VolumeNumber volumeNumber)
        throws AccessDeniedException
    {
        checkDeleted();
        requireAccess(accCtx, AccessType.VIEW);
        return snapshotVlmMap.get(volumeNumber);
    }

    public Collection<SnapshotVolume> getAllSnapshotVolumes(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        requireAccess(accCtx, AccessType.VIEW);
        return snapshotVlmMap.values();
    }

    public void removeSnapshotVolume(
        AccessContext accCtx,
        SnapshotVolumeData snapshotVolumeData
    )
        throws AccessDeniedException
    {
        checkDeleted();
        requireAccess(accCtx, AccessType.CHANGE);
        snapshotVlmMap.remove(snapshotVolumeData.getVolumeNumber());
    }

    public StateFlags<Flags> getFlags()
    {
        checkDeleted();
        return flags;
    }

    public List<DeviceLayerKind> getLayerStack(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        requireAccess(accCtx, AccessType.VIEW);
        return layerStack;
    }

    public void markDeleted(AccessContext accCtx)
        throws AccessDeniedException, DatabaseException
    {
        requireAccess(accCtx, AccessType.USE);
        getFlags().enableFlags(accCtx, Flags.DELETE);
    }

    public void delete(AccessContext accCtx)
        throws AccessDeniedException, DatabaseException
    {
        if (!deleted.get())
        {
            requireAccess(accCtx, AccessType.CONTROL);

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

    public boolean getSuspendResource(AccessContext accCtx)
        throws AccessDeniedException
    {
        requireAccess(accCtx, AccessType.VIEW);
        return suspendResource.get();
    }

    public void setSuspendResource(AccessContext accCtx, boolean suspendResourceRef)
        throws AccessDeniedException
    {
        requireAccess(accCtx, AccessType.CONTROL);
        try
        {
            suspendResource.set(suspendResourceRef);
        }
        catch (DatabaseException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    public boolean getTakeSnapshot(AccessContext accCtx)
        throws AccessDeniedException
    {
        requireAccess(accCtx, AccessType.VIEW);
        return takeSnapshot.get();
    }

    public void setTakeSnapshot(AccessContext accCtx, boolean takeSnapshotRef)
        throws AccessDeniedException
    {
        requireAccess(accCtx, AccessType.CONTROL);
        try
        {
            takeSnapshot.set(takeSnapshotRef);
        }
        catch (DatabaseException exc)
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
    public int compareTo(Snapshot otherSnapshot)
    {
        int eq = getSnapshotDefinition().compareTo(
            otherSnapshot.getSnapshotDefinition()
        );
        if (eq == 0)
        {
            eq = getNode().compareTo(otherSnapshot.getNode());
        }
        return eq;
    }

    private void requireAccess(AccessContext accCtx, AccessType accessType) throws AccessDeniedException
    {
        snapshotDfn.getResourceDefinition().getObjProt().requireAccess(accCtx, accessType);
    }

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

    public NodeName getNodeName()
    {
        return node.getName();
    }

    public ResourceName getResourceName()
    {
        return snapshotDfn.getResourceName();
    }

    public SnapshotName getSnapshotName()
    {
        return snapshotDfn.getName();
    }

    public ResourceDefinition getResourceDefinition()
    {
        return snapshotDfn.getResourceDefinition();
    }
    
    public enum Flags implements  com.linbit.linstor.stateflags.Flags
    {
        DELETE(1L);

        public final long flagValue;

        Flags(long value)
        {
            flagValue = value;
        }

        @Override
        public long getFlagValue()
        {
            return flagValue;
        }

        public static Flags[] restoreFlags(long snapshotFlags)
        {
            List<Flags> flagList = new ArrayList<>();
            for (Flags flag : Flags.values())
            {
                if ((snapshotFlags & flag.flagValue) == flag.flagValue)
                {
                    flagList.add(flag);
                }
            }
            return flagList.toArray(new Flags[0]);
        }

        public static List<String> toStringList(long flagsMask)
        {
            return FlagsHelper.toStringList(Flags.class, flagsMask);
        }

        public static long fromStringList(List<String> listFlags)
        {
            return FlagsHelper.fromStringList(Flags.class, listFlags);
        }
    }
}
