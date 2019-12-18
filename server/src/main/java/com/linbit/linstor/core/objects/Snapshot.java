package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.linstor.api.pojo.SnapshotPojo;
import com.linbit.linstor.core.apis.SnapshotApi;
import com.linbit.linstor.core.apis.SnapshotVolumeApi;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.transaction.TransactionMap;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObject;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;

import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

public class Snapshot extends AbsResource<Snapshot> // TODO: add SnapshotConnection
{
    public static interface InitMaps
    {
        Map<VolumeNumber, SnapshotVolume> getSnapshotVlmMap();
    }

    private final SnapshotDefinition snapshotDfn;

    private final SnapshotDatabaseDriver dbDriver;

    // State flags
    private final StateFlags<Flags> flags;

    // Not persisted because we do not resume snapshot creation after a restart
    private final TransactionSimpleObject<Snapshot, Boolean> suspendResource;

    // Not persisted because we do not resume snapshot creation after a restart
    private final TransactionSimpleObject<Snapshot, Boolean> takeSnapshot;

    private final TransactionMap<VolumeNumber, SnapshotVolume> snapVlmMap;

    public Snapshot(
        UUID objIdRef,
        SnapshotDefinition snapshotDfnRef,
        Node nodeRef,
        long initFlags,
        SnapshotDatabaseDriver dbDriverRef,
        PropsContainerFactory propsConFactory,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProviderRef,
        Map<VolumeNumber, SnapshotVolume> snapshotVlmMapRef
    )
        throws DatabaseException
    {
        super(
            objIdRef,
            nodeRef,
            propsConFactory.getInstance(
                PropsContainer.buildPath(
                    snapshotDfnRef.getResourceName(),
                    snapshotDfnRef.getName()
                )
            ),
            transMgrProviderRef,
            transObjFactory
        );

        snapshotDfn = snapshotDfnRef;
        dbDriver = dbDriverRef;

        flags = transObjFactory.createStateFlagsImpl(
            snapshotDfn.getResourceDefinition().getObjProt(),
            this,
            Flags.class,
            dbDriverRef.getStateFlagsPersistence(),
            initFlags
        );
        snapVlmMap = transObjFactory.createTransactionMap(snapshotVlmMapRef, null);

        suspendResource = transObjFactory.createTransactionSimpleObject(this, false, null);
        takeSnapshot = transObjFactory.createTransactionSimpleObject(this, false, null);

        transObjs.addAll(
            Arrays.asList(
                snapshotDfn,
                snapVlmMap,
                node,
                flags,
                deleted,
                suspendResource,
                takeSnapshot
            )
        );
    }

    public SnapshotDefinition getSnapshotDefinition()
    {
        checkDeleted();
        return snapshotDfn;
    }

    @Override
    public SnapshotVolume getVolume(VolumeNumber volNr)
    {
        checkDeleted();
        return snapVlmMap.get(volNr);
    }

    public int getVolumeCount()
    {
        checkDeleted();
        return snapVlmMap.size();
    }

    public synchronized SnapshotVolume putVolume(AccessContext accCtx, SnapshotVolume vol)
        throws AccessDeniedException
    {
        checkDeleted();
        getObjProt().requireAccess(accCtx, AccessType.CHANGE);

        return snapVlmMap.put(vol.getVolumeNumber(), vol);
    }

    public synchronized void removeVolume(AccessContext accCtx, SnapshotVolume vol)
        throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        getObjProt().requireAccess(accCtx, AccessType.CHANGE);

        VolumeNumber vlmNr = vol.getVolumeNumber();
        snapVlmMap.remove(vlmNr);
        rootLayerData.get().remove(accCtx, vlmNr);
    }

    @Override
    public Iterator<SnapshotVolume> iterateVolumes()
    {
        checkDeleted();
        return Collections.unmodifiableCollection(snapVlmMap.values()).iterator();
    }

    @Override
    public Stream<SnapshotVolume> streamVolumes()
    {
        checkDeleted();
        return Collections.unmodifiableCollection(snapVlmMap.values()).stream();
    }

    public StateFlags<Flags> getFlags()
    {
        checkDeleted();
        return flags;
    }

    @Override
    public void markDeleted(AccessContext accCtx)
        throws AccessDeniedException, DatabaseException
    {
        requireAccess(accCtx, AccessType.USE);
        getFlags().enableFlags(accCtx, Flags.DELETE);
    }

    @Override
    public void delete(AccessContext accCtx)
        throws AccessDeniedException, DatabaseException
    {
        if (!deleted.get())
        {
            requireAccess(accCtx, AccessType.CONTROL);

            snapshotDfn.removeSnapshot(accCtx, this);
            node.removeSnapshot(this);

            // Shallow copy the volume collection because calling delete results in elements being removed from it
            Collection<SnapshotVolume> snapshotVolumes = new ArrayList<>(snapVlmMap.values());
            for (SnapshotVolume snapshotVolume : snapshotVolumes)
            {
                snapshotVolume.delete(accCtx);
            }

            if (rootLayerData.get() != null)
            {
                rootLayerData.get().delete();
            }
            activateTransMgr();
            dbDriver.delete(this);

            deleted.set(true);
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
    public int compareTo(AbsResource<Snapshot> otherSnapshot)
    {
        int eq = -1;
        if (otherSnapshot instanceof Snapshot)
        {
            eq = snapshotDfn.compareTo(((Snapshot) otherSnapshot).snapshotDfn);
            if (eq == 0)
            {
                eq = getNode().compareTo(otherSnapshot.getNode());
            }
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
        List<SnapshotVolumeApi> snapshotVlms = new ArrayList<>();

        for (SnapshotVolume snapshotVolume : snapVlmMap.values())
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
            snapshotVlms,
            getLayerData(accCtx).asPojo(accCtx)
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

    @Override
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
            return flagList.toArray(new Snapshot.Flags[flagList.size()]);
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

    @Override
    public ObjectProtection getObjProt()
    {
        return snapshotDfn.getResourceDefinition().getObjProt();
    }

    public void setSnapshotConnection(AccessContext accCtxRef, TransactionObject rscConRef) throws AccessDeniedException
    {
        throw new ImplementationError("Not implemented yet");
    }

    public Stream<TransactionObject> streamSnapshotConnections(AccessContext accCtxRef) throws AccessDeniedException
    {
        throw new ImplementationError("Not implemented yet");
    }

    public TransactionObject getSnapshotConnection(AccessContext accCtxRef, Snapshot otherRef)
        throws AccessDeniedException
    {
        throw new ImplementationError("Not implemented yet");
    }
}
