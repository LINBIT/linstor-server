package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.interfaces.RscDfnLayerDataApi;
import com.linbit.linstor.api.pojo.SnapshotDfnListItemPojo;
import com.linbit.linstor.api.pojo.SnapshotDfnPojo;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.apis.SnapshotApi;
import com.linbit.linstor.core.apis.SnapshotDefinitionApi;
import com.linbit.linstor.core.apis.SnapshotDefinitionListItemApi;
import com.linbit.linstor.core.apis.SnapshotVolumeDefinitionApi;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotDefinitionDatabaseDriver;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.propscon.ReadOnlyPropsImpl;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.interfaces.categories.resource.RscDfnLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.TransactionList;
import com.linbit.linstor.transaction.TransactionMap;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.utils.Pair;
import com.linbit.utils.PairNonNull;

import jakarta.inject.Provider;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;

public class SnapshotDefinition extends AbsCoreObj<SnapshotDefinition>
{
    public interface InitMaps
    {
        Map<NodeName, Snapshot> getSnapshotMap();
        Map<VolumeNumber, SnapshotVolumeDefinition> getSnapshotVolumeDefinitionMap();
    }


    // Reference to the resource definition
    private final ResourceDefinition resourceDfn;

    private final SnapshotName snapshotName;

    private final SnapshotDefinitionDatabaseDriver dbDriver;

    // Properties container for this snapshot definition
    private final Props snaptDfnProps;
    private final ReadOnlyProps rscDfnRoProps;
    private final Props rscDfnProps;

    // State flags
    private final StateFlags<Flags> flags;

    private final TransactionMap<SnapshotDefinition, VolumeNumber, SnapshotVolumeDefinition> snapshotVolumeDefinitionMap;

    private final TransactionMap<SnapshotDefinition, NodeName, Snapshot> snapshotMap;

    // Not persisted because we do not resume snapshot creation after a restart
    private TransactionSimpleObject<SnapshotDefinition, Boolean> inCreation;

    private final TransactionMap<SnapshotDefinition, PairNonNull<DeviceLayerKind, String>, RscDfnLayerObject> layerStorage;
    private final TransactionList<SnapshotDefinition, DeviceLayerKind> layerStack;

    private final Key snapDfnKey;

    public SnapshotDefinition(
        UUID objIdRef,
        ResourceDefinition resourceDfnRef,
        SnapshotName snapshotNameRef,
        long initFlags,
        SnapshotDefinitionDatabaseDriver dbDriverRef,
        TransactionObjectFactory transObjFactory,
        PropsContainerFactory propsContainerFactory,
        Provider<? extends TransactionMgr> transMgrProviderRef,
        Map<VolumeNumber, SnapshotVolumeDefinition> snapshotVlmDfnMapRef,
        Map<NodeName, Snapshot> snapshotMapRef,
        Map<PairNonNull<DeviceLayerKind, String>, RscDfnLayerObject> layerDataMapRef
    )
        throws DatabaseException
    {
        super(objIdRef, transObjFactory, transMgrProviderRef);
        resourceDfn = resourceDfnRef;
        snapshotName = snapshotNameRef;
        dbDriver = dbDriverRef;

        snapDfnKey = new Key(this);

        snaptDfnProps = propsContainerFactory.getInstance(
            PropsContainer.buildPath(resourceDfn.getName(), snapshotName, false),
            toStringImpl(),
            LinStorObject.SNAP_DFN
        );
        rscDfnProps = propsContainerFactory.getInstance(
            PropsContainer.buildPath(resourceDfn.getName(), snapshotName, true),
            toStringImpl(),
            LinStorObject.RSC_DFN
        );
        rscDfnRoProps = new ReadOnlyPropsImpl(rscDfnProps);

        flags = transObjFactory.createStateFlagsImpl(
            this,
            Flags.class,
            dbDriverRef.getStateFlagsPersistence(),
            initFlags
        );

        snapshotVolumeDefinitionMap = transObjFactory.createTransactionMap(this, snapshotVlmDfnMapRef, null);

        snapshotMap = transObjFactory.createTransactionMap(this, snapshotMapRef, null);

        inCreation = transObjFactory.createTransactionSimpleObject(this, Boolean.FALSE, null);
        layerStorage = transObjFactory.createTransactionMap(this, layerDataMapRef, null);
        layerStack = transObjFactory.createTransactionPrimitiveList(
            this,
            new ArrayList<>(),
            null
        );

        transObjs = Arrays.asList(
            resourceDfn,
            snapshotVolumeDefinitionMap,
            snapshotMap,
            flags,
            layerStorage,
            layerStack,
            deleted,
            inCreation,
            snaptDfnProps,
            rscDfnProps
        );
    }

    public ResourceDefinition getResourceDefinition()
    {
        checkDeleted();
        return resourceDfn;
    }

    public SnapshotName getName()
    {
        checkDeleted();
        return snapshotName;
    }

    public Key getSnapDfnKey()
    {
        // no call to checkDeleted()
        return snapDfnKey;
    }

    public @Nullable SnapshotVolumeDefinition getSnapshotVolumeDefinition(
        VolumeNumber volumeNumber
    )
    {
        checkDeleted();
        return snapshotVolumeDefinitionMap.get(volumeNumber);
    }

    public void addSnapshotVolumeDefinition(
        SnapshotVolumeDefinition snapshotVolumeDefinition
    )
    {
        checkDeleted();
        snapshotVolumeDefinitionMap.put(snapshotVolumeDefinition.getVolumeNumber(), snapshotVolumeDefinition);
    }

    public void removeSnapshotVolumeDefinition(
        VolumeNumber volumeNumber
    )
    {
        checkDeleted();
        snapshotVolumeDefinitionMap.remove(volumeNumber);
    }

    public Collection<SnapshotVolumeDefinition> getAllSnapshotVolumeDefinitions()
    {
        checkDeleted();
        return snapshotVolumeDefinitionMap.values();
    }

    public @Nullable Snapshot getSnapshot(NodeName clNodeName)
    {
        checkDeleted();
        return snapshotMap.get(clNodeName);
    }

    public Collection<Snapshot> getAllSnapshots()
    {
        checkDeleted();
        return snapshotMap.values();
    }

    public Collection<Snapshot> getAllNotDeletingSnapshots()
    {
        checkDeleted();
        TreeSet<Snapshot> ret = new TreeSet<>();
        for (Snapshot snap : snapshotMap.values())
        {
            if (!snap.isDeleted() && snap.getFlags().isUnset(Snapshot.Flags.DELETE))
            {
                ret.add(snap);
            }
        }
        return ret;
    }

    public void addSnapshot(Snapshot snapshotRef)
    {
        checkDeleted();
        snapshotMap.put(snapshotRef.getNodeName(), snapshotRef);
    }

    public void removeSnapshot(Snapshot snapshotRef)
    {
        checkDeleted();
        snapshotMap.remove(snapshotRef.getNodeName());
    }

    public Props getSnapDfnProps()
    {
        checkDeleted();
        return snaptDfnProps;
    }

    public ReadOnlyProps getRscDfnProps()
    {
        checkDeleted();
        return rscDfnRoProps;
    }

    public Props getRscDfnPropsForChange()
    {
        checkDeleted();
        return rscDfnProps;
    }

    public StateFlags<Flags> getFlags()
    {
        checkDeleted();
        return flags;
    }

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

            if (!snapshotMap.isEmpty())
            {
                throw new ImplementationError("Cannot delete snapshot definition which contains snapshots");
            }

            resourceDfn.removeSnapshotDfn(snapshotName);

            // Shallow copy the volume collection because calling delete results in elements being removed from it
            Collection<SnapshotVolumeDefinition> snapshotVolumeDefinitions =
                new ArrayList<>(snapshotVolumeDefinitionMap.values());
            for (SnapshotVolumeDefinition snapshotVolumeDefinition : snapshotVolumeDefinitions)
            {
                snapshotVolumeDefinition.delete();
            }

            snaptDfnProps.delete();
            rscDfnProps.delete();

            for (RscDfnLayerObject rscDfnLayerObject : layerStorage.values())
            {
                rscDfnLayerObject.delete();
            }


            activateTransMgr();
            dbDriver.delete(this);

            deleted.set(Boolean.TRUE);
        }
    }

    public void setInCreation(boolean inCreationRef)
        throws DatabaseException
    {
        checkDeleted();
        inCreation.set(inCreationRef);
    }

    @SuppressWarnings("unchecked")
    public <T extends RscDfnLayerObject> T setLayerData(T rscDfnLayerData)
        throws DatabaseException
    {
        checkDeleted();
        return (T) layerStorage.put(
            new PairNonNull<>(
                rscDfnLayerData.getLayerKind(),
                rscDfnLayerData.getRscNameSuffix()
            ),
            rscDfnLayerData
        );
    }

    /**
     * Returns a single RscDfnLayerObject matching the kind as well as the resourceNameSuffix.
     */
    @SuppressWarnings({"unchecked", "TypeParameterUnusedInFormals"})
    public <T extends RscDfnLayerObject> @Nullable T getLayerData(
        DeviceLayerKind kind,
        String rscNameSuffixRef
    )
    {
        checkDeleted();
        return (T) layerStorage.get(new PairNonNull<>(kind, rscNameSuffixRef));
    }

    /**
     * Returns a {@code Map<ResourceNameSuffix, RscDfnLayerObject>} where the RscDfnLayerObject has
     * the same DeviceLayerKind as the given argument
     *
     */
    @SuppressWarnings("unchecked")
    public <T extends RscDfnLayerObject> Map<String, T> getLayerData(
        DeviceLayerKind kind
    )
    {
        checkDeleted();

        Map<String, T> ret = new TreeMap<>();
        for (Entry<PairNonNull<DeviceLayerKind, String>, RscDfnLayerObject> entry : layerStorage.entrySet())
        {
            PairNonNull<DeviceLayerKind, String> key = entry.getKey();
            if (key.objA.equals(kind))
            {
                ret.put(key.objB, (T) entry.getValue());
            }
        }
        return ret;
    }

    public void removeLayerData(
        DeviceLayerKind kind,
        String rscNameSuffixRef
    )
        throws DatabaseException
    {
        checkDeleted();
        layerStorage.remove(new PairNonNull<>(kind, rscNameSuffixRef)).delete();
        for (SnapshotVolumeDefinition snapVlmDfn : snapshotVolumeDefinitionMap.values())
        {
            snapVlmDfn.removeLayerData(kind, rscNameSuffixRef);
        }
    }

    public void setLayerStack(List<DeviceLayerKind> list)
    {
        checkDeleted();
        layerStack.clear();
        layerStack.addAll(list);
    }

    public List<DeviceLayerKind> getLayerStack()
    {
        checkDeleted();
        return layerStack;
    }

    private List<SnapshotApi> getSnapshotApis()
    {
        checkDeleted();
        List<SnapshotApi> snapshotApis = new ArrayList<>();
        for (Snapshot snap : snapshotMap.values())
        {
            snapshotApis.add(snap.getApiData(0L, 0L));
        }
        return snapshotApis;
    }

    public SnapshotDefinitionApi getApiData(boolean withSnapshots)
    {
        checkDeleted();
        List<SnapshotVolumeDefinitionApi> snapshotVlmDfns = new ArrayList<>();

        for (SnapshotVolumeDefinition snapshotVolumeDefinition : snapshotVolumeDefinitionMap.values())
        {
            snapshotVlmDfns.add(snapshotVolumeDefinition.getApiData());
        }

        /*
         * This serialization is only for clients (satellite will not iterate over this set)
         * Sorting an enum by default orders by its ordinal number, not alphanumerically.
         */
        TreeSet<PairNonNull<DeviceLayerKind, String>> sortedLayerStack = new TreeSet<>();
        for (DeviceLayerKind kind : layerStack)
        {
            sortedLayerStack.add(new PairNonNull<>(kind, ""));
        }
        sortedLayerStack.addAll(layerStorage.keySet());

        List<Pair<String, RscDfnLayerDataApi>> layerData = new ArrayList<>();
        for (PairNonNull<DeviceLayerKind, String> pair : sortedLayerStack)
        {
            RscDfnLayerObject rscDfnLayerObject = layerStorage.get(pair);
            layerData.add(
                new Pair<>(
                    pair.objA.name(),
                    rscDfnLayerObject == null ? null : rscDfnLayerObject.getApiData()
                )
            );
        }

        return new SnapshotDfnPojo(
            resourceDfn.getApiData(),
            objId,
            snapshotName.getDisplayName(),
            snapshotVlmDfns,
            flags.getFlagsBits(),
            snaptDfnProps.cloneMap(),
            rscDfnProps.cloneMap(),
            layerData,
            withSnapshots ? getSnapshotApis() : Collections.emptyList()
        );
    }

    public SnapshotDefinitionListItemApi getListItemApiData()
    {
        checkDeleted();
        return new SnapshotDfnListItemPojo(
            getApiData(true),
            snapshotMap.values().stream()
                .map(Snapshot::getNodeName)
                .map(NodeName::getDisplayName)
                .collect(Collectors.toList()),
            getSnapshotApis()
        );
    }

    /**
     * Get the average creation time of all snapshots of this snapDfn
     *
     */
    public @Nullable Instant getCreationTime()
    {
        checkDeleted();
        int count = 0;
        long sum = 0;
        for (Snapshot snap : snapshotMap.values())
        {
            Optional<Instant> crtTs = snap.getCreateTimestamp();
            if (crtTs.isPresent())
            {
                sum += crtTs.get().toEpochMilli();
                count++;
            }
        }
        return count == 0 ? null : Instant.ofEpochMilli(sum / count);
    }

    @Override
    public String toStringImpl()
    {
        return "Rsc: '" + snapDfnKey.resourceName + "', Snapshot: '" + snapshotName + "'";
    }

    @Override
    public int compareTo(SnapshotDefinition otherSnapshotDfn)
    {
        int eq = getResourceDefinition().compareTo(otherSnapshotDfn.getResourceDefinition());
        if (eq == 0)
        {
            eq = getName().compareTo(otherSnapshotDfn.getName());
        }
        return eq;
    }

    @Override
    public int hashCode()
    {
        checkDeleted();
        return Objects.hash(resourceDfn, snapshotName);
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
        else if (obj instanceof SnapshotDefinition other)
        {
            other.checkDeleted();
            ret = Objects.equals(resourceDfn, other.resourceDfn) && Objects.equals(snapshotName, other.snapshotName);
        }
        return ret;
    }

    /**
     * Identifies a snapshot definition.
     */
    public static class Key implements Comparable<Key>
    {
        private final ResourceName resourceName;

        private final SnapshotName snapshotName;

        public Key(SnapshotDefinition snapshotDefinition)
        {
            this(snapshotDefinition.getResourceName(), snapshotDefinition.getName());
        }

        public Key(ResourceName resourceNameRef, SnapshotName snapshotNameRef)
        {
            resourceName = resourceNameRef;
            snapshotName = snapshotNameRef;
        }

        public ResourceName getResourceName()
        {
            return resourceName;
        }

        public SnapshotName getSnapshotName()
        {
            return snapshotName;
        }

        // Code style exception: Automatically generated code
        @Override
        @SuppressWarnings(
            {
                "DescendantToken", "ParameterName"
            }
        )
        public boolean equals(Object o)
        {
            if (this == o)
            {
                return true;
            }
            if (!(o instanceof Key that))
            {
                return false;
            }
            return Objects.equals(resourceName, that.resourceName) &&
                Objects.equals(snapshotName, that.snapshotName);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(resourceName, snapshotName);
        }

        @Override
        @SuppressWarnings("unchecked")
        public int compareTo(Key other)
        {
            int eq = resourceName.compareTo(other.resourceName);
            if (eq == 0)
            {
                eq = snapshotName.compareTo(other.snapshotName);
            }
            return eq;
        }
    }

    public ResourceName getResourceName()
    {
        return resourceDfn.getName();
    }

    public enum Flags implements com.linbit.linstor.stateflags.Flags
    {
        SUCCESSFUL(1L << 0),
        FAILED_DEPLOYMENT(1L << 1),
        FAILED_DISCONNECT(1L << 2),
        DELETE(1L << 3),
        @Deprecated
        SHIPPING(1L << 4),
        @Deprecated
        SHIPPING_CLEANUP(1L << 5),
        @Deprecated
        SHIPPING_ABORT(1L << 6),
        @Deprecated
        SHIPPED(1L << 7),
        AUTO_SNAPSHOT(1L << 8),
        @Deprecated
        BACKUP(1L << 9),
        @Deprecated
        RESTORE_BACKUP_ON_SUCCESS(1L << 10),
        @Deprecated
        FORCE_RESTORE_BACKUP_ON_SUCCESS(1L << 11 | RESTORE_BACKUP_ON_SUCCESS.flagValue),
        @Deprecated
        PREPARE_SHIPPING_ABORT(1L << 12),
        SAFETY_SNAPSHOT(1L << 13),
        ;

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

        public static Flags[] restoreFlags(long snapshotDfnFlags)
        {
            List<Flags> flagList = new ArrayList<>();
            for (Flags flag : Flags.values())
            {
                if ((snapshotDfnFlags & flag.flagValue) == flag.flagValue)
                {
                    flagList.add(flag);
                }
            }
            return flagList.toArray(new Flags[flagList.size()]);
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
