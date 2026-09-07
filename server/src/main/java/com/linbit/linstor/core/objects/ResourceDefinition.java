package com.linbit.linstor.core.objects;

import com.linbit.ErrorCheck;
import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.interfaces.RscDfnLayerDataApi;
import com.linbit.linstor.api.pojo.RscDfnPojo;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.apis.ResourceDefinitionApi;
import com.linbit.linstor.core.apis.VolumeDefinitionApi;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDefinitionDatabaseDriver;
import com.linbit.linstor.layer.storage.BlockSizeConsts;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.satellitestate.SatelliteResourceState;
import com.linbit.linstor.satellitestate.SatelliteState;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.interfaces.categories.resource.RscDfnLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.TransactionList;
import com.linbit.linstor.transaction.TransactionMap;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.linstor.utils.layer.LayerKindUtils;
import com.linbit.locks.LockGuard;
import com.linbit.utils.MathUtils;
import com.linbit.utils.Pair;
import com.linbit.utils.PairNonNull;

import jakarta.inject.Provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Represents a resource definition in the LINSTOR cluster.
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class ResourceDefinition extends AbsCoreObj<ResourceDefinition>
{
    public interface InitMaps
    {
        Map<NodeName, Resource> getRscMap();
        Map<VolumeNumber, VolumeDefinition> getVlmDfnMap();
        Map<SnapshotName, SnapshotDefinition> getSnapshotDfnMap();
    }

    // Resource name
    private final ResourceName resourceName;

    // User suggested name
    private final @Nullable byte[] externalName;

    // Volumes of the resource
    private final TransactionMap<ResourceDefinition, VolumeNumber, VolumeDefinition> volumeMap;

    // Resources defined by this ResourceDefinition
    private final TransactionMap<ResourceDefinition, NodeName, Resource> resourceMap;

    // Snapshots from this resource definition
    private final TransactionMap<ResourceDefinition, SnapshotName, SnapshotDefinition> snapshotDfnMap;

    // State flags
    private final StateFlags<Flags> flags;

    // Properties container for this resource definition
    private final Props rscDfnProps;

    private final ResourceDefinitionDatabaseDriver dbDriver;

    private final TransactionMap<ResourceDefinition, PairNonNull<DeviceLayerKind, String>, RscDfnLayerObject> layerStorage;

    private final TransactionList<ResourceDefinition, @Nullable DeviceLayerKind> layerStack;

    private final TransactionSimpleObject<ResourceDefinition, ResourceGroup> rscGrp;

    ResourceDefinition(
        UUID objIdRef,
        ResourceName resName,
        @Nullable byte[] extName,
        long initialFlags,
        List<DeviceLayerKind> layerStackRef,
        ResourceDefinitionDatabaseDriver dbDriverRef,
        PropsContainerFactory propsContainerFactory,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProviderRef,
        Map<VolumeNumber, VolumeDefinition> vlmDfnMapRef,
        Map<NodeName, Resource> rscMapRef,
        Map<SnapshotName, SnapshotDefinition> snapshotDfnMapRef,
        Map<PairNonNull<DeviceLayerKind, String>, RscDfnLayerObject> layerDataMapRef,
        ResourceGroup rscGrpRef
    )
        throws DatabaseException
    {
        super(objIdRef, transObjFactory, transMgrProviderRef);

        ErrorCheck.ctorNotNull(ResourceDefinition.class, ResourceName.class, resName);
        ErrorCheck.ctorNotNull(ResourceDefinition.class, ResourceGroup.class, rscGrpRef);
        resourceName = resName;
        externalName = extName;
        dbDriver = dbDriverRef;
        volumeMap = transObjFactory.createTransactionMap(this, vlmDfnMapRef, null);
        resourceMap = transObjFactory.createTransactionMap(this, rscMapRef, null);
        snapshotDfnMap = transObjFactory.createTransactionMap(this, snapshotDfnMapRef, null);
        layerStack = transObjFactory.createTransactionPrimitiveList(
            this,
            layerStackRef,
            dbDriver.getLayerStackDriver()
        );
        rscGrp = transObjFactory.createTransactionSimpleObject(this, rscGrpRef, dbDriver.getRscGrpDriver());

        rscDfnProps = propsContainerFactory.getInstance(
            PropsContainer.buildPath(resName),
            toStringImpl(),
            LinStorObject.RSC_DFN
        );
        flags = transObjFactory.createStateFlagsImpl(
            this,
            Flags.class,
            dbDriver.getStateFlagsPersistence(),
            initialFlags
        );

        layerStorage = transObjFactory.createTransactionMap(this, layerDataMapRef, null);

        transObjs = Arrays.asList(
            flags,
            volumeMap,
            resourceMap,
            rscDfnProps,
            layerStack,
            layerStorage,
            rscGrp,
            deleted
        );
    }

    public ResourceName getName()
    {
        checkDeleted();
        return resourceName;
    }

    public @Nullable byte[] getExternalName()
    {
        checkDeleted();
        return externalName;
    }

    public Props getProps()
    {
        checkDeleted();
        return rscDfnProps;
    }

    /**
     * Sets a property for the device manager to restart the DRBD resource on each resource of this resource definition
     *
     * @throws DatabaseException if a database operation fails
     */
    public void requireDrbdRestart()
        throws DatabaseException
    {
        final Iterator<Resource> rscIter = iterateResource();
        while (rscIter.hasNext())
        {
            final Resource rsc = rscIter.next();
            rsc.requireDrbdRestart();
        }
    }

    public synchronized void putVolumeDefinition(VolumeDefinition volDfn)
    {
        checkDeleted();
        volumeMap.put(volDfn.getVolumeNumber(), volDfn);
    }

    public synchronized void removeVolumeDefinition(VolumeDefinition volDfn)
    {
        checkDeleted();
        volumeMap.remove(volDfn.getVolumeNumber());
    }

    public int getVolumeDfnCount()
    {
        checkDeleted();
        return volumeMap.size();
    }

    public @Nullable VolumeDefinition getVolumeDfn(VolumeNumber volNr)
    {
        checkDeleted();
        return volumeMap.get(volNr);
    }

    public Iterator<VolumeDefinition> iterateVolumeDfn()
    {
        checkDeleted();
        return volumeMap.values().iterator();
    }

    public Stream<VolumeDefinition> streamVolumeDfn()
    {
        checkDeleted();
        return volumeMap.values().stream();
    }

    /**
     * Returns the smallest min-io-size used by any of the volume definitions of this resource definition
     * or <code>null</code> if this resource-definition does not have any resources.
     *
     * If the resource definition's layer stack lists any special layers, the result is
     * BlockSizeConsts.DFLT_SPECIAL_PHY_IO_SIZE. Otherwise, the result is the smallest minimum I/O size currently
     * set on any of the volume definitions of this resource definition.
     *
     * @return Floor value minimum-io-size of all volume definitions, or <code>null</code> if no resources exist
     */
    public @Nullable Long getFloorVolumesMinIoSize(boolean autoMinIoSizeRef)
    {
        @Nullable Long ret;
        if (resourceMap.isEmpty())
        {
            ret = null;
        }
        else
        {
            if (LayerKindUtils.hasSpecialLayers(this))
            {
                ret = BlockSizeConsts.DFLT_SPECIAL_PHY_IO_SIZE;
            }
            else
            {
                @Nullable Long floorMinIoSize;
                @Nullable Long dfltVlmDfnMinIoSize;
                if (autoMinIoSizeRef)
                {
                    floorMinIoSize = BlockSizeConsts.MAX_PHY_IO_SIZE;
                    dfltVlmDfnMinIoSize = BlockSizeConsts.DFLT_PHY_IO_SIZE;
                }
                else
                {
                    floorMinIoSize = null;
                    dfltVlmDfnMinIoSize = null;
                }
                final Iterator<VolumeDefinition> vlmIter = iterateVolumeDfn();
                while (vlmIter.hasNext())
                {
                    final VolumeDefinition vlmDfn = vlmIter.next();
                    final @Nullable Long vlmMinIoSize = vlmDfn.getMinIoSize(dfltVlmDfnMinIoSize);
                    if (vlmMinIoSize != null && (floorMinIoSize == null || vlmMinIoSize < floorMinIoSize))
                    {
                        floorMinIoSize = vlmMinIoSize;
                    }
                }
                if (floorMinIoSize != null)
                {
                    ret = MathUtils.bounds(BlockSizeConsts.MIN_PHY_IO_SIZE, floorMinIoSize, BlockSizeConsts.MAX_PHY_IO_SIZE);
                }
                else
                {
                    ret = null;
                }
            }
        }
        return ret;
    }

    public int getResourceCount()
    {
        checkDeleted();
        return resourceMap.size();
    }

    public int getDiskfulCount()
    {
        return getDiskfulResources().size();
    }

    /**
     * <p>Returns resources that have a <b>local</b> disk. Excludes {@link Resource.Flags#DRBD_DISKLESS},
     * {@link Resource.Flags#NVME_INITIATOR} and {@link Resource.Flags#EBS_INITIATOR}</p>
     */
    public List<Resource> getDiskfulResources()
    {
        return getResourcesFilteredByFlags(
            rscFlags -> rscFlags.isUnset(
                Resource.Flags.DRBD_DISKLESS,
                Resource.Flags.NVME_INITIATOR,
                Resource.Flags.EBS_INITIATOR
            )
        );
    }

    /**
     * <p>Returns resources that have a <b>no local</b> disk. Includes {@link Resource.Flags#DRBD_DISKLESS},
     * {@link Resource.Flags#NVME_INITIATOR} and {@link Resource.Flags#EBS_INITIATOR}</p>
     */
    public List<Resource> getDisklessResources()
    {
        return getResourcesFilteredByFlags(
            rscFlags -> rscFlags.isSomeSet(
                Resource.Flags.DRBD_DISKLESS,
                Resource.Flags.NVME_INITIATOR,
                Resource.Flags.EBS_INITIATOR
            )
        );
    }

    public int getNotDeletedDiskfulCountExcluding(Resource... rscsRef)
    {
        List<Resource> rscList = getNotDeletedDiskful();
        rscList.removeAll(Arrays.asList(rscsRef));
        return rscList.size();
    }

    public int getNotDeletedDiskfulCount()
    {
        return getNotDeletedDiskful().size();
    }

    public List<Resource> getNotDeletedDiskful()
    {
        return getResourcesFilteredByFlags(
            rscFlags -> rscFlags.isUnset(
                Resource.Flags.DELETE,
                Resource.Flags.DRBD_DELETE,
                Resource.Flags.DRBD_DISKLESS,
                Resource.Flags.NVME_INITIATOR,
                Resource.Flags.EBS_INITIATOR
            )
        );
    }

    private List<Resource> getResourcesFilteredByFlags(Predicate<StateFlags<Resource.Flags>> flagFilter)
    {
        return getFilteredResources(rsc -> flagFilter.test(rsc.getStateFlags()));
    }

    private List<Resource> getFilteredResources(Predicate<Resource> filterRef)
    {
        checkDeleted();
        var resources = new ArrayList<Resource>();
        Iterator<Resource> rscIt = iterateResource();
        while (rscIt.hasNext())
        {
            Resource rsc = rscIt.next();
            if (filterRef.test(rsc))
            {
                resources.add(rsc);
            }
        }
        return resources;
    }

    public Iterator<Resource> iterateResource()
    {
        checkDeleted();
        return resourceMap.values().iterator();
    }

    public Stream<Resource> streamResource()
    {
        checkDeleted();
        return resourceMap.values().stream();
    }

    public Map<NodeName, Resource> copyResourceMap()
    {
        return copyResourceMap(null);
    }

    public Map<NodeName, Resource> copyResourceMap(
        @Nullable Map<NodeName, Resource> dstMap
    )
    {
        checkDeleted();
        Map<NodeName, Resource> targetMap = dstMap == null ? new TreeMap<>() : dstMap;
        targetMap.putAll(resourceMap);
        return targetMap;
    }

    public @Nullable Resource getResource(NodeName clNodeName)
    {
        checkDeleted();
        return resourceMap.get(clNodeName);
    }

    public void addResource(Resource resRef)
    {
        checkDeleted();

        resourceMap.put(resRef.getNode().getName(), resRef);
    }

    void removeResource(Resource resRef)
    {
        checkDeleted();

        resourceMap.remove(resRef.getNode().getName());
    }

    public void addSnapshotDfn(SnapshotDefinition snapshotDfn)
    {
        checkDeleted();

        snapshotDfnMap.put(snapshotDfn.getName(), snapshotDfn);
    }

    public @Nullable SnapshotDefinition getSnapshotDfn(SnapshotName snapshotName)
    {
        checkDeleted();
        return snapshotDfnMap.get(snapshotName);
    }

    public Collection<SnapshotDefinition> getSnapshotDfns()
    {
        checkDeleted();
        return snapshotDfnMap.values();
    }

    public void removeSnapshotDfn(SnapshotName snapshotName)
    {
        checkDeleted();

        snapshotDfnMap.remove(snapshotName);
    }

    public StateFlags<Flags> getFlags()
    {
        checkDeleted();
        return flags;
    }

    public boolean hasDiskless()
    {
        checkDeleted();
        boolean hasDiskless = false;
        for (Resource rsc : streamResource().collect(Collectors.toList()))
        {
            StateFlags<Resource.Flags> stateFlags = rsc.getStateFlags();
            hasDiskless = stateFlags.isSomeSet(
                Resource.Flags.DRBD_DISKLESS,
                Resource.Flags.NVME_INITIATOR,
                Resource.Flags.EBS_INITIATOR
            );
            if (hasDiskless)
            {
                break;
            }
        }
        return hasDiskless;
    }

    public boolean hasDisklessNotDeleting()
    {
        checkDeleted();
        boolean hasDisklessNotDeleting = false;
        for (Resource rsc : streamResource().collect(Collectors.toList()))
        {
            StateFlags<Resource.Flags> stateFlags = rsc.getStateFlags();
            boolean isDiskless = stateFlags.isSomeSet(
                Resource.Flags.DRBD_DISKLESS,
                Resource.Flags.NVME_INITIATOR,
                Resource.Flags.EBS_INITIATOR
            );
            boolean isNotDeleting = stateFlags.isUnset(Resource.Flags.DELETE);
            if (isDiskless && isNotDeleting)
            {
                hasDisklessNotDeleting = true;
                break;
            }
        }
        return hasDisklessNotDeleting;
    }

    public void markDeleted() throws DatabaseException
    {
        getFlags().enableFlags(Flags.DELETE);
    }

    @SuppressWarnings("unchecked")
    public <T extends RscDfnLayerObject> T setLayerData(T rscDfnLayerData)
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
    public @Nullable <T extends RscDfnLayerObject> T getLayerData(
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
        for (VolumeDefinition vlmDfn : volumeMap.values())
        {
            vlmDfn.removeLayerData(kind, rscNameSuffixRef);
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

    public boolean usesLayer(DeviceLayerKind kindRef)
    {
        checkDeleted();
        return !getLayerData(kindRef).isEmpty();
    }

    public void setResourceGroup(ResourceGroup rscGrpRef)
        throws DatabaseException
    {
        checkDeleted();
        rscGrp.get().removeResourceDefinition(this);
        rscGrp.set(rscGrpRef);
        rscGrp.get().addResourceDefinition(this);
    }

    public ResourceGroup getResourceGroup()
    {
        checkDeleted();
        return rscGrp.get();
    }

    @Override
    public void delete()
        throws DatabaseException
    {
        if (!deleted.get())
        {
            if (!snapshotDfnMap.isEmpty())
            {
                throw new ImplementationError("Cannot delete resource definition which contains snapshot definitions");
            }

            if (!resourceMap.isEmpty())
            {
                throw new ImplementationError("Cannot delete resource definition which contains resources");
            }

            // Shallow copy the volume definition collection because calling delete results in elements being removed
            // from it
            Collection<VolumeDefinition> volumeDefinitions = new ArrayList<>(volumeMap.values());
            for (VolumeDefinition volumeDefinition : volumeDefinitions)
            {
                volumeDefinition.delete();
            }

            rscDfnProps.delete();

            for (RscDfnLayerObject rscDfnLayerObject : layerStorage.values())
            {
                rscDfnLayerObject.delete();
            }

            rscGrp.get().removeResourceDefinition(this);


            activateTransMgr();
            dbDriver.delete(this);

            deleted.set(true);
        }
    }

    public ResourceDefinitionApi getApiData()
    {
        checkDeleted();
        ArrayList<VolumeDefinitionApi> vlmDfnList = new ArrayList<>();
        Iterator<VolumeDefinition> vlmDfnIter = iterateVolumeDfn();
        while (vlmDfnIter.hasNext())
        {
            VolumeDefinition vd = vlmDfnIter.next();
            vlmDfnList.add(vd.getApiData());
        }

        /*
         * Satellite should not care about this layerData (and especially not about its ordering)
         * as the satellite should only take the resource's tree-structured layerData into account
         *
         * This means, this serialization is basically only for clients.
         *
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

        return new RscDfnPojo(
            getUuid(),
            getResourceGroup().getApiData(),
            getName().getDisplayName(),
            getExternalName(),
            getFlags().getFlagsBits(),
            getProps().cloneMap(),
            vlmDfnList,
            layerData
        );
    }

    /**
     * Checks if any resource in the definition is currently used (mounted).
     * Returns an Optional<Resource> object containing the resources that is mounted or an empty.
     *
     * @return The first found mounted/primary resource, if none is mounted returns empty optional.
     */
    public Optional<Resource> anyResourceInUse()
    {
        checkDeleted();
        Resource rscInUse = null;
        Iterator<Resource> rscInUseIterator = iterateResource();
        while (rscInUseIterator.hasNext() && rscInUse == null)
        {
            Resource rsc = rscInUseIterator.next();

            Peer nodePeer = rsc.getNode().getPeer();
            @Nullable Boolean inUse = null;
            try (LockGuard ignored = LockGuard.createLocked(nodePeer.getSatelliteStateLock().readLock()))
            {
                @Nullable SatelliteState satelliteState = nodePeer.getSatelliteState();
                if (satelliteState != null)
                {
                    inUse = satelliteState.getFromResource(resourceName, SatelliteResourceState::isInUse);
                }
            }
            if (inUse != null && inUse)
            {
                rscInUse = rsc;
            }
        }
        return Optional.ofNullable(rscInUse);
    }

    @Override
    public String toStringImpl()
    {
        return "RscDfn: '" + resourceName + "'";
    }

    @Override
    public int compareTo(ResourceDefinition otherRscDfn)
    {
        return getName().compareTo(otherRscDfn.getName());
    }

    @Override
    public int hashCode()
    {
        checkDeleted();
        return Objects.hash(resourceName);
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
        else if (obj instanceof ResourceDefinition other)
        {
            other.checkDeleted();
            ret = Objects.equals(resourceName, other.resourceName);
        }
        return ret;
    }

    public enum Flags implements com.linbit.linstor.stateflags.Flags
    {
        DELETE(1L),
        RESTORE_TARGET(1L << 1),
        CLONING(1L << 2),
        FAILED(1L << 3);

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

        public static Flags[] restoreFlags(long rawFlags)
        {
            List<Flags> list = new ArrayList<>();
            for (Flags flag : Flags.values())
            {
                if ((rawFlags & flag.flagValue) == flag.flagValue)
                {
                    list.add(flag);
                }
            }
            return list.toArray(new Flags[list.size()]);
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
