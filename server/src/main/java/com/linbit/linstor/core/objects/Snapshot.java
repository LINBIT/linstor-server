package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.pojo.SnapshotPojo;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.apis.SnapshotApi;
import com.linbit.linstor.core.apis.SnapshotVolumeApi;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotDatabaseDriver;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.propscon.ReadOnlyPropsImpl;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.transaction.TransactionMap;
import com.linbit.linstor.transaction.TransactionObject;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import jakarta.inject.Provider;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Stream;

public class Snapshot extends AbsResource<Snapshot> // TODO: add SnapshotConnection
{
    public interface InitMaps
    {
        Map<VolumeNumber, SnapshotVolume> getSnapshotVlmMap();
    }

    private final SnapshotDefinition snapshotDfn;

    private final SnapshotDatabaseDriver dbDriver;

    private final Props snapProps;
    private final ReadOnlyProps rscRoProps;
    private final Props rscProps;

    // State flags
    private final StateFlags<Flags> flags;

    // Not persisted because we do not resume snapshot creation after a restart
    private final TransactionSimpleObject<Snapshot, Boolean> suspendResource;

    // Not persisted because we do not resume snapshot creation after a restart
    private final TransactionSimpleObject<Snapshot, Boolean> takeSnapshot;
    // Not persisted because we do not resume snapshot creation after a restart
    private final TransactionSimpleObject<Snapshot, Boolean> shipBackup;

    private final TransactionMap<Snapshot, VolumeNumber, SnapshotVolume> snapVlmMap;

    private final Key snapKey;

    public Snapshot(
        UUID objIdRef,
        SnapshotDefinition snapshotDfnRef,
        Node nodeRef,
        long initFlags,
        SnapshotDatabaseDriver dbDriverRef,
        PropsContainerFactory propsConFactory,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProviderRef,
        Map<VolumeNumber, SnapshotVolume> snapshotVlmMapRef,
        @Nullable Instant createTimestampRef
    )
        throws DatabaseException
    {
        super(
            objIdRef,
            nodeRef,
            transMgrProviderRef,
            transObjFactory,
            createTimestampRef,
            dbDriverRef
        );

        snapshotDfn = snapshotDfnRef;
        dbDriver = dbDriverRef;
        snapKey = new Key(this);

        snapProps = propsConFactory.getInstance(
            PropsContainer.buildPath(
                nodeRef.getName(),
                snapshotDfnRef.getResourceName(),
                snapshotDfnRef.getName(),
                false
            ),
            toStringImpl() + " [Snap]",
            LinStorObject.SNAP
        );

        rscProps = propsConFactory.getInstance(
            PropsContainer.buildPath(
                nodeRef.getName(),
                snapshotDfnRef.getResourceName(),
                snapshotDfnRef.getName(),
                true
            ),
            toStringImpl() + " [Snap.Rsc]",
            LinStorObject.RSC
        );
        rscRoProps = new ReadOnlyPropsImpl(rscProps);

        flags = transObjFactory.createStateFlagsImpl(
            this,
            Flags.class,
            dbDriverRef.getStateFlagsPersistence(),
            initFlags
        );
        snapVlmMap = transObjFactory.createTransactionMap(this, snapshotVlmMapRef, null);

        suspendResource = transObjFactory.createTransactionSimpleObject(this, false, null);
        takeSnapshot = transObjFactory.createTransactionSimpleObject(this, false, null);
        shipBackup = transObjFactory.createTransactionSimpleObject(this, false, null);

        transObjs.addAll(
            Arrays.asList(
                snapshotDfn,
                snapVlmMap,
                node,
                snapProps,
                rscProps,
                flags,
                deleted,
                suspendResource,
                takeSnapshot,
                shipBackup
            )
        );
    }

    public SnapshotDefinition getSnapshotDefinition()
    {
        checkDeleted();
        return snapshotDfn;
    }

    public Key getSnapshotKey()
    {
        // no check deleted
        return snapKey;
    }

    public Props getSnapProps()
    {
        checkDeleted();
        return snapProps;
    }

    public ReadOnlyProps getRscProps()
    {
        checkDeleted();
        return rscRoProps;
    }

    public Props getRscPropsForChange()
    {
        checkDeleted();
        return rscProps;
    }

    @Override
    public @Nullable SnapshotVolume getVolume(VolumeNumber volNr)
    {
        checkDeleted();
        return snapVlmMap.get(volNr);
    }

    public int getVolumeCount()
    {
        checkDeleted();
        return snapVlmMap.size();
    }

    public synchronized SnapshotVolume putVolume(SnapshotVolume vol)
    {
        checkDeleted();

        return snapVlmMap.put(vol.getVolumeNumber(), vol);
    }

    public synchronized void removeVolume(SnapshotVolume vol)
        throws DatabaseException
    {
        checkDeleted();

        VolumeNumber vlmNr = vol.getVolumeNumber();
        snapVlmMap.remove(vlmNr);
        rootLayerData.get().remove(vlmNr);
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
    public void markDeleted()
        throws DatabaseException
    {
        getFlags().enableFlags(Flags.DELETE);
    }

    @Override
    public void delete()
        throws DatabaseException
    {
        if (!deleted.get())
        {

            snapshotDfn.removeSnapshot(this);
            node.removeSnapshot(this);

            // Shallow copy the volume collection because calling delete results in elements being removed from it
            Collection<SnapshotVolume> snapshotVolumes = new ArrayList<>(snapVlmMap.values());
            for (SnapshotVolume snapshotVolume : snapshotVolumes)
            {
                snapshotVolume.delete();
            }

            if (rootLayerData.get() != null)
            {
                rootLayerData.get().delete();
            }

            snapProps.delete();
            rscProps.delete();

            activateTransMgr();
            dbDriver.delete(this);

            deleted.set(true);
        }
    }

    public boolean getSuspendResource()
    {
        return suspendResource.get();
    }

    public void setSuspendResource(boolean suspendResourceRef)
    {
        try
        {
            suspendResource.set(suspendResourceRef);
        }
        catch (DatabaseException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    public boolean getTakeSnapshot()
    {
        return takeSnapshot.get();
    }

    public void setTakeSnapshot(boolean takeSnapshotRef)
    {
        try
        {
            takeSnapshot.set(takeSnapshotRef);
        }
        catch (DatabaseException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    public boolean getShipBackup()
    {
        return shipBackup.get();
    }

    public void setShipBackup(boolean shipBackupRef)
    {
        try
        {
            shipBackup.set(shipBackupRef);
        }
        catch (DatabaseException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    @Override
    public String toStringImpl()
    {
        return String.format("Node: '%s', Rsc: '%s', Snapshot: '%s'", snapKey.nodeName, snapKey.resourceName, snapKey.snapshotName);
    }

    @Override
    public int compareTo(AbsResource<Snapshot> otherSnapshot)
    {
        int eq = -1;
        if (otherSnapshot instanceof Snapshot snapshot)
        {
            eq = snapshotDfn.compareTo(snapshot.snapshotDfn);
            if (eq == 0)
            {
                eq = getNode().compareTo(otherSnapshot.getNode());
            }
        }
        return eq;
    }

    @Override
    public int hashCode()
    {
        checkDeleted();
        return Objects.hash(snapshotDfn, node);
    }

    @Override
    public boolean equals(Object obj)
    {
        checkDeleted();
        boolean ret = false;
        if (this == obj)
        {
            ret = true;
        }
        else if (obj instanceof Snapshot other)
        {
            other.checkDeleted();
            ret = Objects.equals(snapshotDfn, other.snapshotDfn) && Objects.equals(node, other.node);
        }
        return ret;
    }

    public SnapshotApi getApiData(@Nullable Long fullSyncId, @Nullable Long updateId)
    {
        checkDeleted();
        List<SnapshotVolumeApi> snapshotVlms = new ArrayList<>();

        for (SnapshotVolume snapshotVolume : snapVlmMap.values())
        {
            snapshotVlms.add(snapshotVolume.getApiData());
        }

        return new SnapshotPojo(
            snapshotDfn.getApiData(false),
            objId,
            flags.getFlagsBits(),
            suspendResource.get(),
            takeSnapshot.get(),
            fullSyncId,
            updateId,
            snapshotVlms,
            getLayerData().asPojo(),
            getNodeName().displayValue,
            getCreateTimestamp().orElse(null),
            snapProps.cloneMap(),
            rscRoProps.cloneMap(),
            shipBackup.get()
        );
    }

    public NodeName getNodeName()
    {
        checkDeleted();
        return node.getName();
    }

    public ResourceName getResourceName()
    {
        checkDeleted();
        return snapshotDfn.getResourceName();
    }

    public SnapshotName getSnapshotName()
    {
        checkDeleted();
        return snapshotDfn.getName();
    }

    @Override
    public ResourceDefinition getResourceDefinition()
    {
        checkDeleted();
        return snapshotDfn.getResourceDefinition();
    }

    public enum Flags implements  com.linbit.linstor.stateflags.Flags
    {
        DELETE(1L << 0),
        @Deprecated
        SHIPPING_SOURCE(1L << 1),
        @Deprecated
        SHIPPING_SOURCE_START(SHIPPING_SOURCE.flagValue | 1L << 2),
        @Deprecated
        SHIPPING_SOURCE_DONE(SHIPPING_SOURCE.flagValue | 1L << 3),
        @Deprecated
        SHIPPING_TARGET(1L << 4),
        @Deprecated
        SHIPPING_TARGET_CLEANING_UP(SHIPPING_TARGET.flagValue | 1L << 5),
        @Deprecated
        SHIPPING_TARGET_DONE(SHIPPING_TARGET.flagValue | 1L << 6),
        @Deprecated
        BACKUP_SOURCE(1L << 7),
        @Deprecated
        BACKUP_TARGET(1L << 8);

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

    public void setSnapshotConnection(TransactionObject rscConRef)
    {
        throw new ImplementationError("Not implemented yet");
    }

    public Stream<TransactionObject> streamSnapshotConnections()
    {
        throw new ImplementationError("Not implemented yet");
    }

    public TransactionObject getSnapshotConnection(Snapshot otherRef)
    {
        throw new ImplementationError("Not implemented yet");
    }

    /**
     * Identifies a snapshot.
     */
    public static class Key implements Comparable<Key>
    {
        private final ResourceName resourceName;

        private final SnapshotName snapshotName;

        private final NodeName nodeName;

        public Key(Snapshot snap)
        {
            this(snap.getResourceName(), snap.getSnapshotName(), snap.getNodeName());
        }

        public Key(ResourceName resourceNameRef, SnapshotName snapshotNameRef, NodeName nodeNameRef)
        {
            resourceName = resourceNameRef;
            snapshotName = snapshotNameRef;
            nodeName = nodeNameRef;
        }

        public ResourceName getResourceName()
        {
            return resourceName;
        }

        public SnapshotName getSnapshotName()
        {
            return snapshotName;
        }

        public NodeName getNodeName()
        {
            return nodeName;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(nodeName, resourceName, snapshotName);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
            {
                return true;
            }
            if (!(obj instanceof Key other))
            {
                return false;
            }
            return Objects.equals(nodeName, other.nodeName) && Objects.equals(resourceName, other.resourceName) &&
                Objects.equals(snapshotName, other.snapshotName);
        }

        @Override
        public int compareTo(Key other)
        {
            int eq = nodeName.compareTo(other.nodeName);
            if (eq == 0)
            {
                eq = resourceName.compareTo(other.resourceName);
                if (eq == 0)
                {
                    eq = snapshotName.compareTo(other.snapshotName);
                }
            }
            return eq;
        }
    }
}
