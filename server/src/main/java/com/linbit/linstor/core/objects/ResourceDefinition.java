package com.linbit.linstor.core.objects;

import com.linbit.ErrorCheck;
import com.linbit.linstor.AccessToDeletedDataException;
import com.linbit.linstor.DbgInstanceUuid;
import com.linbit.linstor.api.interfaces.RscDfnLayerDataApi;
import com.linbit.linstor.api.pojo.RscDfnPojo;
import com.linbit.linstor.core.apis.ResourceDefinitionApi;
import com.linbit.linstor.core.apis.VolumeDefinitionApi;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDefinitionDatabaseDriver;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsAccess;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.satellitestate.SatelliteResourceState;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ProtectedObject;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.interfaces.categories.resource.RscDfnLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionList;
import com.linbit.linstor.transaction.TransactionMap;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;
import com.linbit.locks.LockGuard;
import com.linbit.utils.Pair;

import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class ResourceDefinition extends BaseTransactionObject
    implements DbgInstanceUuid, Comparable<ResourceDefinition>, ProtectedObject
{
    public static interface InitMaps
    {
        Map<NodeName, Resource> getRscMap();
        Map<VolumeNumber, VolumeDefinition> getVlmDfnMap();
        Map<SnapshotName, SnapshotDefinition> getSnapshotDfnMap();
    }

    // Object identifier
    private final UUID objId;

    // Runtime instance identifier for debug purposes
    private final transient UUID dbgInstanceId;

    // Resource name
    private final ResourceName resourceName;

    // User suggested name
    private final byte[] externalName;

    // Volumes of the resource
    private final TransactionMap<VolumeNumber, VolumeDefinition> volumeMap;

    // Resources defined by this ResourceDefinition
    private final TransactionMap<NodeName, Resource> resourceMap;

    // Snapshots from this resource definition
    private final TransactionMap<SnapshotName, SnapshotDefinition> snapshotDfnMap;

    // State flags
    private final StateFlags<Flags> flags;

    // Object access controls
    private final ObjectProtection objProt;

    // Properties container for this resource definition
    private final Props rscDfnProps;

    private final ResourceDefinitionDatabaseDriver dbDriver;

    private final TransactionMap<Pair<DeviceLayerKind, String>, RscDfnLayerObject> layerStorage;

    private final TransactionSimpleObject<ResourceDefinition, Boolean> deleted;

    private final TransactionList<ResourceDefinition, DeviceLayerKind> layerStack;

    private final ResourceGroup rscGrp;

    ResourceDefinition(
        UUID objIdRef,
        ObjectProtection objProtRef,
        ResourceName resName,
        byte[] extName,
        long initialFlags,
        List<DeviceLayerKind> layerStackRef,
        ResourceDefinitionDatabaseDriver dbDriverRef,
        PropsContainerFactory propsContainerFactory,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProviderRef,
        Map<VolumeNumber, VolumeDefinition> vlmDfnMapRef,
        Map<NodeName, Resource> rscMapRef,
        Map<SnapshotName, SnapshotDefinition> snapshotDfnMapRef,
        Map<Pair<DeviceLayerKind, String>, RscDfnLayerObject> layerDataMapRef,
        ResourceGroup rscGrpRef
    )
        throws DatabaseException
    {
        super(transMgrProviderRef);

        ErrorCheck.ctorNotNull(ResourceDefinition.class, ResourceName.class, resName);
        ErrorCheck.ctorNotNull(ResourceDefinition.class, ObjectProtection.class, objProtRef);
        ErrorCheck.ctorNotNull(ResourceDefinition.class, ResourceGroup.class, rscGrpRef);
        objId = objIdRef;
        dbgInstanceId = UUID.randomUUID();
        objProt = objProtRef;
        resourceName = resName;
        externalName = extName;
        dbDriver = dbDriverRef;
        volumeMap = transObjFactory.createTransactionMap(vlmDfnMapRef, null);
        resourceMap = transObjFactory.createTransactionMap(rscMapRef, null);
        snapshotDfnMap = transObjFactory.createTransactionMap(snapshotDfnMapRef, null);
        layerStack = transObjFactory.createTransactionPrimitiveList(
            this,
            layerStackRef,
            dbDriver.getLayerStackDriver()
        );
        deleted = transObjFactory.createTransactionSimpleObject(this, false, null);
        rscGrp = rscGrpRef;

        rscDfnProps = propsContainerFactory.getInstance(
            PropsContainer.buildPath(resName)
        );
        flags = transObjFactory.createStateFlagsImpl(
            objProt,
            this,
            Flags.class,
            dbDriver.getStateFlagsPersistence(),
            initialFlags
        );

        layerStorage = transObjFactory.createTransactionMap(layerDataMapRef, null);

        transObjs = Arrays.asList(
            flags,
            objProt,
            volumeMap,
            resourceMap,
            rscDfnProps,
            layerStack,
            layerStorage,
            rscGrp,
            deleted
        );
    }

    public UUID getUuid()
    {
        checkDeleted();
        return objId;
    }

    public ResourceName getName()
    {
        checkDeleted();
        return resourceName;
    }

    public byte[] getExternalName()
    {
        checkDeleted();
        return externalName;
    }

    public Props getProps(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        return PropsAccess.secureGetProps(accCtx, objProt, rscDfnProps);
    }

    public synchronized void putVolumeDefinition(AccessContext accCtx, VolumeDefinition volDfn)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);
        volumeMap.put(volDfn.getVolumeNumber(), volDfn);
    }

    public synchronized void removeVolumeDefinition(AccessContext accCtx, VolumeDefinition volDfn)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);
        volumeMap.remove(volDfn.getVolumeNumber());
    }

    public int getVolumeDfnCount(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return volumeMap.size();
    }

    public VolumeDefinition getVolumeDfn(AccessContext accCtx, VolumeNumber volNr)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return volumeMap.get(volNr);
    }

    public Iterator<VolumeDefinition> iterateVolumeDfn(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return volumeMap.values().iterator();
    }

    public Stream<VolumeDefinition> streamVolumeDfn(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return volumeMap.values().stream();
    }

    public int getResourceCount()
    {
        return resourceMap.size();
    }

    public int diskfullCount(AccessContext accCtx) throws AccessDeniedException
    {
        int count = 0;
        for (Resource rsc : streamResource(accCtx).collect(Collectors.toList()))
        {
            StateFlags<Resource.Flags> stateFlags = rsc.getStateFlags();
            if (
                !stateFlags.isSet(accCtx, Resource.Flags.DRBD_DISKLESS) &&
                !stateFlags.isSet(accCtx, Resource.Flags.NVME_INITIATOR))
            {
                count++;
            }
        }
        return count;
    }

    public Iterator<Resource> iterateResource(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return resourceMap.values().iterator();
    }

    public Stream<Resource> streamResource(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return resourceMap.values().stream();
    }

    public void copyResourceMap(
        AccessContext accCtx, Map<NodeName, ? super Resource> dstMap
    )
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        dstMap.putAll(resourceMap);
    }

    @Override
    public ObjectProtection getObjProt()
    {
        checkDeleted();
        return objProt;
    }

    public Resource getResource(AccessContext accCtx, NodeName clNodeName)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return resourceMap.get(clNodeName);
    }

    public void addResource(AccessContext accCtx, Resource resRef) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);

        resourceMap.put(resRef.getNode().getName(), resRef);
    }

    void removeResource(AccessContext accCtx, Resource resRef) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);

        resourceMap.remove(resRef.getNode().getName());
    }

    public void addSnapshotDfn(AccessContext accCtx, SnapshotDefinition snapshotDfn)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);

        snapshotDfnMap.put(snapshotDfn.getName(), snapshotDfn);
    }

    public SnapshotDefinition getSnapshotDfn(AccessContext accCtx, SnapshotName snapshotName)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return snapshotDfnMap.get(snapshotName);
    }

    public Collection<SnapshotDefinition> getSnapshotDfns(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return snapshotDfnMap.values();
    }

    public void removeSnapshotDfn(AccessContext accCtx, SnapshotName snapshotName)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);

        snapshotDfnMap.remove(snapshotName);
    }

    public StateFlags<Flags> getFlags()
    {
        checkDeleted();
        return flags;
    }

    public boolean hasDiskless(AccessContext accCtx) throws AccessDeniedException
    {
        boolean hasDiskless = false;
        for (Resource rsc : streamResource(accCtx).collect(Collectors.toList()))
        {
            StateFlags<Resource.Flags> stateFlags = rsc.getStateFlags();
            hasDiskless = stateFlags.isSet(accCtx, Resource.Flags.DRBD_DISKLESS) ||
                stateFlags.isSet(accCtx, Resource.Flags.NVME_INITIATOR);
            if (hasDiskless)
            {
                break;
            }
        }
        return hasDiskless;
    }

    public boolean hasDisklessNotDeleting(AccessContext accCtx) throws AccessDeniedException
    {
        boolean hasDisklessNotDeleting = false;
        for (Resource rsc : streamResource(accCtx).collect(Collectors.toList()))
        {
            StateFlags<Resource.Flags> stateFlags = rsc.getStateFlags();
            if (
                (stateFlags.isSet(accCtx, Resource.Flags.DRBD_DISKLESS) ||
                 stateFlags.isSet(accCtx, Resource.Flags.NVME_INITIATOR)
                ) &&
                stateFlags.isUnset(accCtx, Resource.Flags.DELETE))
            {
                hasDisklessNotDeleting = true;
                break;
            }
        }
        return hasDisklessNotDeleting;
    }

    public void markDeleted(AccessContext accCtx) throws AccessDeniedException, DatabaseException
    {
        getFlags().enableFlags(accCtx, Flags.DELETE);
    }

    @SuppressWarnings("unchecked")
    public <T extends RscDfnLayerObject> T setLayerData(AccessContext accCtx, T rscDfnLayerData)
        throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);
        return (T) layerStorage.put(
            new Pair<>(
                rscDfnLayerData.getLayerKind(),
                rscDfnLayerData.getRscNameSuffix()
            ),
            rscDfnLayerData
        );
    }

    /**
     * Returns a single RscDfnLayerObject matching the kind as well as the resourceNameSuffix.
     */
    @SuppressWarnings("unchecked")
    public <T extends RscDfnLayerObject> T getLayerData(
        AccessContext accCtx,
        DeviceLayerKind kind,
        String rscNameSuffixRef
    )
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);
        return (T) layerStorage.get(new Pair<>(kind, rscNameSuffixRef));
    }

    /**
     * Returns a map of <ResourceNameSuffix, RscDfnLayerObject> where the RscDfnLayerObject has
     * the same DeviceLayerKind as the given argument
     *
     * @throws AccessDeniedException
     */
    @SuppressWarnings("unchecked")
    public <T extends RscDfnLayerObject> Map<String, T> getLayerData(
        AccessContext accCtx,
        DeviceLayerKind kind
    )
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);

        Map<String, T> ret = new TreeMap<>();
        for (Entry<Pair<DeviceLayerKind, String>, RscDfnLayerObject> entry : layerStorage.entrySet())
        {
            Pair<DeviceLayerKind, String> key = entry.getKey();
            if (key.objA.equals(kind))
            {
                ret.put(key.objB, (T) entry.getValue());
            }
        }
        return ret;
    }

    public void removeLayerData(
        AccessContext accCtx,
        DeviceLayerKind kind,
        String rscNameSuffixRef
    )
        throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);
        layerStorage.remove(new Pair<>(kind, rscNameSuffixRef)).delete();
        for (VolumeDefinition vlmDfn : volumeMap.values())
        {
            vlmDfn.removeLayerData(accCtx, kind, rscNameSuffixRef);
        }
    }

    public void setLayerStack(AccessContext accCtx, List<DeviceLayerKind> list)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        layerStack.clear();
        layerStack.addAll(list);
    }

    public List<DeviceLayerKind> getLayerStack(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        return layerStack;
    }

    public ResourceGroup getResourceGroup()
    {
        checkDeleted();
        return rscGrp;
    }

    public void delete(AccessContext accCtx)
        throws AccessDeniedException, DatabaseException
    {
        if (!deleted.get())
        {
            objProt.requireAccess(accCtx, AccessType.CONTROL);

            // Shallow copy the resource collection because calling delete results in elements being removed from it
            Collection<Resource> resources = new ArrayList<>(resourceMap.values());
            for (Resource rsc : resources)
            {
                rsc.delete(accCtx);
            }

            // Shallow copy the volume definition collection because calling delete results in elements being removed
            // from it
            Collection<VolumeDefinition> volumeDefinitions = new ArrayList<>(volumeMap.values());
            for (VolumeDefinition volumeDefinition : volumeDefinitions)
            {
                volumeDefinition.delete(accCtx);
            }

            rscDfnProps.delete();

            for (RscDfnLayerObject rscDfnLayerObject : layerStorage.values())
            {
                rscDfnLayerObject.delete();
            }

            rscGrp.removeResourceDefinition(accCtx, this);

            objProt.delete(accCtx);

            activateTransMgr();
            dbDriver.delete(this);

            deleted.set(true);
        }
    }

    public boolean isDeleted()
    {
        return deleted.get();
    }

    private void checkDeleted()
    {
        if (isDeleted())
        {
            throw new AccessToDeletedDataException("Access to deleted resource definition");
        }
    }

    public ResourceDefinitionApi getApiData(AccessContext accCtx)
        throws AccessDeniedException
    {
        ArrayList<VolumeDefinitionApi> vlmDfnList = new ArrayList<>();
        Iterator<VolumeDefinition> vlmDfnIter = iterateVolumeDfn(accCtx);
        while (vlmDfnIter.hasNext())
        {
            VolumeDefinition vd = vlmDfnIter.next();
            vlmDfnList.add(vd.getApiData(accCtx));
        }

        /*
         * Satellite should not care about this layerData (and especially not about its ordering)
         * as the satellite should only take the resource's tree-structured layerData into account
         *
         * This means, this serialization is basically only for clients.
         *
         * Sorting an enum by default orders by its ordinal number, not alphanumerically.
         */
        TreeSet<Pair<DeviceLayerKind, String>> sortedLayerStack = new TreeSet<>();
        for (DeviceLayerKind kind : layerStack)
        {
            sortedLayerStack.add(new Pair<>(kind, ""));
        }
        sortedLayerStack.addAll(layerStorage.keySet());

        List<Pair<String, RscDfnLayerDataApi>> layerData = new ArrayList<>();
        for (Pair<DeviceLayerKind, String> pair : sortedLayerStack)
        {
            RscDfnLayerObject rscDfnLayerObject = layerStorage.get(pair);
            layerData.add(
                new Pair<>(
                    pair.objA.name(),
                    rscDfnLayerObject == null ? null : rscDfnLayerObject.getApiData(accCtx)
                )
            );
        }

        return new RscDfnPojo(
            getUuid(),
            getResourceGroup().getApiData(accCtx),
            getName().getDisplayName(),
            getExternalName(),
            getFlags().getFlagsBits(accCtx),
            getProps(accCtx).map(),
            vlmDfnList,
            layerData
        );
    }

    /**
     * Checks if any resource in the definition is currently used (mounted).
     * Returns an Optional<Resource> object containing the resources that is mounted or an empty.
     *
     * @param accCtx
     *     AccessContext for checks
     * @return The first found mounted/primary resource, if none is mounted returns empty optional.
     * @throws AccessDeniedException
     */
    public Optional<Resource> anyResourceInUse(AccessContext accCtx)
        throws AccessDeniedException
    {
        Resource rscInUse = null;
        Iterator<Resource> rscInUseIterator = iterateResource(accCtx);
        while (rscInUseIterator.hasNext() && rscInUse == null)
        {
            Resource rsc = rscInUseIterator.next();

            Peer nodePeer = rsc.getNode().getPeer(accCtx);
            if (nodePeer != null)
            {
                Boolean inUse;
                try (LockGuard ignored = LockGuard.createLocked(nodePeer.getSatelliteStateLock().readLock()))
                {
                    inUse = nodePeer.getSatelliteState().getFromResource(resourceName, SatelliteResourceState::isInUse);
                }
                if (inUse != null && inUse)
                {
                    rscInUse = rsc;
                }
            }
        }
        return Optional.ofNullable(rscInUse);
    }

    @Override
    public String toString()
    {
        return "Rsc: '" + resourceName + "'";
    }

    @Override
    public UUID debugGetVolatileUuid()
    {
        return dbgInstanceId;
    }

    @Override
    public int compareTo(ResourceDefinition otherRscDfn)
    {
        return getName().compareTo(otherRscDfn.getName());
    }

    public enum Flags implements com.linbit.linstor.stateflags.Flags
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
