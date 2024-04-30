package com.linbit.linstor.core.objects;

import com.linbit.ErrorCheck;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.PriorityProps.MultiResult;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.pojo.EffectivePropertiesPojo;
import com.linbit.linstor.api.pojo.RscPojo;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.apis.ResourceApi;
import com.linbit.linstor.core.apis.ResourceConnectionApi;
import com.linbit.linstor.core.apis.VolumeApi;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.transaction.TransactionMap;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.linstor.utils.layer.LayerVlmUtils;

import javax.annotation.Nullable;
import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Representation of a resource
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class Resource extends AbsResource<Resource>
{
    public interface InitMaps
    {
        Map<ResourceKey, ResourceConnection> getRscConnMap();
        Map<VolumeNumber, Volume> getVlmMap();
    }

    private static final String TO_STRING_FORMAT = "Node: '%s', Rsc: '%s'";

    // Reference to the resource definition
    private final ResourceDefinition resourceDfn;

    // State flags
    private final StateFlags<Flags> flags;

    // Connections to the peer resources
    private final TransactionMap<Resource, Resource.ResourceKey, ResourceConnection> resourceConnections;
    private final TransactionMap<Resource, VolumeNumber, Volume> vlmMap;

    // Access control for this resource
    private final ObjectProtection objProt;

    private final ResourceDatabaseDriver dbDriver;


    private boolean createPrimary = false;

    private final ResourceKey rscKey;


    Resource(
        UUID objIdRef,
        ObjectProtection objProtRef,
        ResourceDefinition resDfnRef,
        Node nodeRef,
        long initFlags,
        ResourceDatabaseDriver dbDriverRef,
        PropsContainerFactory propsContainerFactory,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProviderRef,
        Map<Resource.ResourceKey, ResourceConnection> rscConnMapRef,
        Map<VolumeNumber, Volume> vlmMapRef,
        Date createTimestampRef
    )
        throws DatabaseException
    {
        super(
            objIdRef,
            nodeRef,
            propsContainerFactory.getInstance(
                PropsContainer.buildPath(
                    nodeRef.getName(),
                    resDfnRef.getName()
                ),
                String.format(TO_STRING_FORMAT, nodeRef.getName(), resDfnRef.getName()),
                LinStorObject.RESOURCE
            ),
            transMgrProviderRef,
            transObjFactory,
            createTimestampRef,
            dbDriverRef
        );
        dbDriver = dbDriverRef;

        ErrorCheck.ctorNotNull(Resource.class, ResourceDefinition.class, resDfnRef);
        resourceDfn = resDfnRef;
        resourceConnections = transObjFactory.createTransactionMap(this, rscConnMapRef, null);
        objProt = objProtRef;
        vlmMap = transObjFactory.createTransactionMap(this, vlmMapRef, null);

        flags = transObjFactory.createStateFlagsImpl(
            objProt,
            this,
            Flags.class,
            dbDriver.getStateFlagPersistence(),
            initFlags
        );

        rscKey = new ResourceKey(this);

        transObjs.addAll(
            Arrays.asList(
                vlmMap,
                resourceConnections,
                resourceDfn,
                flags,
                objProt
            )
        );
    }

    @Override
    public ObjectProtection getObjProt()
    {
        checkDeleted();
        return objProt;
    }

    @Override
    public ResourceDefinition getResourceDefinition()
    {
        checkDeleted();
        return resourceDfn;
    }

    @Override
    public Volume getVolume(VolumeNumber volNr)
    {
        checkDeleted();
        return vlmMap.get(volNr);
    }

    public int getVolumeCount()
    {
        checkDeleted();
        return vlmMap.size();
    }

    public synchronized Volume putVolume(AccessContext accCtx, Volume vol)
        throws AccessDeniedException
    {
        checkDeleted();
        getObjProt().requireAccess(accCtx, AccessType.CHANGE);

        return vlmMap.put(vol.getVolumeNumber(), vol);
    }

    public synchronized void removeVolume(AccessContext accCtx, Volume vol)
        throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        getObjProt().requireAccess(accCtx, AccessType.CHANGE);

        VolumeNumber vlmNr = vol.getVolumeNumber();
        vlmMap.remove(vlmNr);
        rootLayerData.get().remove(accCtx, vlmNr);
    }

    @Override
    public Iterator<Volume> iterateVolumes()
    {
        checkDeleted();
        return Collections.unmodifiableCollection(vlmMap.values()).iterator();
    }

    @Override
    public Stream<Volume> streamVolumes()
    {
        checkDeleted();
        return Collections.unmodifiableCollection(vlmMap.values()).stream();
    }

    public synchronized void setAbsResourceConnection(AccessContext accCtx, ResourceConnection resCon)
        throws AccessDeniedException
    {
        checkDeleted();

        Resource sourceResource = resCon.getSourceResource(accCtx);
        Resource targetResource = resCon.getTargetResource(accCtx);

        sourceResource.getObjProt().requireAccess(accCtx, AccessType.USE);
        targetResource.getObjProt().requireAccess(accCtx, AccessType.USE);

        if (this == sourceResource)
        {
            resourceConnections.put(targetResource.getKey(), resCon);
        }
        else
        {
            resourceConnections.put(sourceResource.getKey(), resCon);
        }
    }

    public synchronized void removeResourceConnection(AccessContext accCtx, ResourceConnection con)
        throws AccessDeniedException
    {
        checkDeleted();
        Resource sourceResource = con.getSourceResource(accCtx);
        Resource targetResource = con.getTargetResource(accCtx);

        sourceResource.getObjProt().requireAccess(accCtx, AccessType.USE);
        targetResource.getObjProt().requireAccess(accCtx, AccessType.USE);

        if (this == sourceResource)
        {
            resourceConnections.remove(targetResource.getKey());
        }
        else
        {
            resourceConnections.remove(sourceResource.getKey());
        }
    }

    public Stream<ResourceConnection> streamAbsResourceConnections(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return resourceConnections.values().stream();
    }

    public ResourceConnection getAbsResourceConnection(AccessContext accCtx, Resource otherResource)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return resourceConnections.get(otherResource.getKey());
    }


    public StateFlags<Flags> getStateFlags()
    {
        checkDeleted();
        return flags;
    }

    /**
     * Whether peers should treat this resource as diskless.
     */
    public boolean disklessForDrbdPeers(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        return flags.isSet(accCtx, Flags.DRBD_DISKLESS) &&
            flags.isUnset(accCtx, Flags.DISK_ADDING) &&
            flags.isUnset(accCtx, Flags.DISK_REMOVING);
    }

    @Override
    public void markDeleted(AccessContext accCtx) throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);
        getStateFlags().enableFlags(accCtx, Flags.DELETE);
    }

    public void markDrbdDeleted(AccessContext accCtx) throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);
        getStateFlags().enableFlags(accCtx, Flags.DRBD_DELETE);
    }

    @Override
    public void delete(AccessContext accCtx)
        throws AccessDeniedException, DatabaseException
    {
        if (!deleted.get())
        {
            objProt.requireAccess(accCtx, AccessType.CONTROL);

            node.removeResource(accCtx, this);
            resourceDfn.removeResource(accCtx, this);

            // preventing ConcurrentModificationException
            Collection<ResourceConnection> rscConnValues = new ArrayList<>(resourceConnections.values());
            for (ResourceConnection rscConn : rscConnValues)
            {
                rscConn.delete(accCtx);
            }

            // preventing ConcurrentModificationException
            Collection<AbsVolume<Resource>> vlmValues = new ArrayList<>(vlmMap.values());
            for (AbsVolume<Resource> vlm : vlmValues)
            {
                vlm.delete(accCtx);
            }

            props.delete();

            objProt.delete(accCtx);

            if (rootLayerData.get() != null)
            {
                rootLayerData.get().delete(accCtx);
            }

            activateTransMgr();
            dbDriver.delete(this);

            deleted.set(true);
        }
    }

    public void setCreatePrimary()
    {
        checkDeleted();
        createPrimary = true;
    }

    public void unsetCreatePrimary()
    {
        checkDeleted();
        createPrimary = false;
    }

    public boolean isCreatePrimary()
    {
        checkDeleted();
        return createPrimary;
    }

    public boolean isDrbdDiskless(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        return getStateFlags().isSet(accCtx, Flags.DRBD_DISKLESS);
    }

    public boolean isNvmeInitiator(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        return getStateFlags().isSet(accCtx, Flags.NVME_INITIATOR);
    }

    public boolean isEbsInitiator(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        return getStateFlags().isSet(accCtx, Flags.EBS_INITIATOR);
    }

    public boolean isDiskless(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        return isDrbdDiskless(accCtx) || isNvmeInitiator(accCtx);
    }

    public ResourceApi getApiData(
        AccessContext accCtx,
        Long fullSyncId,
        Long updateId,
        @Nullable EffectivePropertiesPojo effectiveProps
    )
        throws AccessDeniedException
    {
        checkDeleted();
        List<VolumeApi> volumes = new ArrayList<>();
        Iterator<Volume> itVolumes = iterateVolumes();
        while (itVolumes.hasNext())
        {
            volumes.add(itVolumes.next().getApiData(null, accCtx));
        }
        List<ResourceConnectionApi> rscConns = new ArrayList<>();
        for (ResourceConnection rscConn : streamAbsResourceConnections(accCtx).collect(Collectors.toList()))
        {
            rscConns.add(rscConn.getApiData(accCtx));
        }
        return new RscPojo(
            getResourceDefinition().getName().getDisplayName(),
            getNode().getName().getDisplayName(),
            getNode().getUuid(),
            getResourceDefinition().getApiData(accCtx),
            getUuid(),
            getStateFlags().getFlagsBits(accCtx),
            getProps(accCtx).map(),
            volumes,
            null, // otherRscList
            rscConns,
            fullSyncId,
            updateId,
            getLayerData(accCtx).asPojo(accCtx),
            getCreateTimestamp().orElse(null),
            effectiveProps
        );
    }

    public EffectivePropertiesPojo getEffectiveProps(
        AccessContext accCtx,
        StltConfigAccessor stltCfgAccessor
    ) throws AccessDeniedException
    {
        // get prio-props
        PriorityProps prioProps = new PriorityProps(getProps(accCtx));
        for (StorPool storPool : LayerVlmUtils.getStorPools(this, accCtx))
        {
            prioProps.addProps(storPool.getProps(accCtx));
        }
        prioProps.addProps(
            getNode().getProps(accCtx),
            getResourceDefinition().getProps(accCtx),
            stltCfgAccessor.getReadonlyProps()
        );
        // get conflicting map
        Map<String, MultiResult> conflictingProps = prioProps.renderConflictingMap(
            ApiConsts.NAMESPC_DRBD_OPTIONS,
            true
        );
        // remove all props from result that do not use this prioProps-structure
        List<String> applicablePropsKeys = Arrays.asList("DrbdOptions/SkipDisk");
        conflictingProps.keySet().retainAll(applicablePropsKeys);
        // turn into pojo
        return EffectivePropertiesPojo.build(conflictingProps);
    }

    /**
     * Returns the identification key without checking if "this" is already deleted
     */
    public ResourceKey getKey()
    {
        // no call to checkDeleted
        return rscKey;
    }

    @Override
    public int compareTo(AbsResource<Resource> otherRsc)
    {
        int eq = 1;
        if (otherRsc instanceof Resource)
        {
            eq = getNode().compareTo(otherRsc.getNode());
            if (eq == 0)
            {
                eq = getResourceDefinition().compareTo(((Resource) otherRsc).getResourceDefinition());
            }
        }
        return eq;
    }

    @Override
    public int hashCode()
    {
        checkDeleted();
        return Objects.hash(rscKey);
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
        else if (obj instanceof Resource)
        {
            Resource other = (Resource) obj;
            other.checkDeleted();
            ret = Objects.equals(rscKey, other.rscKey);
        }
        return ret;
    }
    @Override
    public String toStringImpl()
    {
        return String.format(TO_STRING_FORMAT, rscKey.nodeName, rscKey.resourceName);
    }

    public static String getStringId(Resource rsc)
    {
        return rsc.getNode().getName().value + "/" +
            rsc.getResourceDefinition().getName().value;
    }

    /**
     * Identifies a resource globally.
     */
    public static class ResourceKey implements Comparable<ResourceKey>
    {
        private final NodeName nodeName;
        private final ResourceName resourceName;

        public ResourceKey(Resource resource)
        {
            this(resource.getNode().getName(), resource.getResourceDefinition().getName());
        }

        public ResourceKey(NodeName nodeNameRef, ResourceName resourceNameRef)
        {
            resourceName = resourceNameRef;
            nodeName = nodeNameRef;
        }

        public NodeName getNodeName()
        {
            return nodeName;
        }

        public ResourceName getResourceName()
        {
            return resourceName;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
            {
                return true;
            }
            if (obj == null || getClass() != obj.getClass())
            {
                return false;
            }
            ResourceKey that = (ResourceKey) obj;
            return Objects.equals(nodeName, that.nodeName) &&
                Objects.equals(resourceName, that.resourceName);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(nodeName, resourceName);
        }

        @Override
        public int compareTo(ResourceKey other)
        {
            int eq = nodeName.compareTo(other.nodeName);
            if (eq == 0)
            {
                eq = resourceName.compareTo(other.resourceName);
            }
            return eq;
        }

        @Override
        public String toString()
        {
            return "Resource.Key [Node: " + nodeName + ", Resource: " + resourceName + "]";
        }
    }

    public enum Flags implements com.linbit.linstor.stateflags.Flags
    {
        CLEAN(1L << 0),
        DELETE(1L << 1),
        @Deprecated
        DISKLESS(1L << 2),
        DISK_ADD_REQUESTED(1L << 3),
        DISK_ADDING(1L << 4),
        DISK_REMOVE_REQUESTED(1L << 5),
        DISK_REMOVING(1L << 6),

        DRBD_DISKLESS(DISKLESS.flagValue | 1L << 8),
        // DO NOT rename TIE_BREAKER to DRBD_TIE_BREAKER for compatibility reasons
        TIE_BREAKER(DRBD_DISKLESS.flagValue | 1L << 7),

        NVME_INITIATOR(DISKLESS.flagValue | 1L << 9),

        /**
         * Causes the controller to set the "Resource inactive" ignore reason to be set on all layers except the storage
         * layer. That will cause the layers to be skipped within the device manager run
         */
        INACTIVE(1L << 10),
        /**
         * Causes the satellite to re-decrypt LUKS volume keys and such. Should be a short state
         */
        REACTIVATE(1L << 11),
        /**
         * Same as {@link #INACTIVE} but ... permanently (who would have guessed)?
         * A resource will be declared permanently inactive once it is used as a snapshot-shipping target.
         * The reason for this is that the received snapshot will (except for external metadata) contain the internal
         * metadata from the sender. Rolling back to such a state (activating the resource first) would cause the
         * target-resource to have the same metadata as the once-sender. We also cannot recreate the metadata since in
         * that case we would lose the tracking of which data we might need to send to new peers.
         */
        INACTIVE_PERMANENTLY(INACTIVE.flagValue | 1L << 12),
        @Deprecated
        BACKUP_RESTORE(1L << 13),
        EVICTED(INACTIVE.flagValue | 1L << 14),
        INACTIVE_BEFORE_EVICTION(1L << 15),
        RESTORE_FROM_SNAPSHOT(1L << 16),
        EVACUATE(1L << 17),
        DRBD_DELETE(1L << 18),
        /**
         * Causes the satellite to deactivate all layers (except storage). Once the satellite are finished with
         * inactivating, the {@link #INACTIVE} flag should be placed. Should be a short state
         */
        INACTIVATING(1L << 19),
        EBS_INITIATOR(1L << 20 | DISKLESS.getFlagValue()),
        AUTO_DISKFUL(1L << 21),
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

        public static Flags[] restoreFlags(long rscFlags)
        {
            List<Flags> flagList = new ArrayList<>();
            for (Flags flag : Flags.values())
            {
                if ((rscFlags & flag.flagValue) == flag.flagValue)
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

        public static Flags valueOfOrNull(String str)
        {
            Flags ret = null;
            for (Flags flag : Flags.values())
            {
                if (flag.name().equalsIgnoreCase(str))
                {
                    ret = flag;
                    break;
                }
            }
            return ret;
        }
    }

    public enum DiskfulBy
    {
        USER("user"),
        AUTO_PLACER("auto-placer"),
        AUTO_DISKFUL("auto-diskful"),
        MAKE_AVAILABLE("make-available");

        private String value;

        DiskfulBy(String valueRef)
        {
            value = valueRef;
        }

        public String getValue()
        {
            return value;
        }
    }
}
