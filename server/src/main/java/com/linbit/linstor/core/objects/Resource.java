package com.linbit.linstor.core.objects;

import com.linbit.ErrorCheck;
import com.linbit.linstor.api.pojo.RscPojo;
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
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
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
    public static interface InitMaps
    {
        Map<ResourceKey, ResourceConnection> getRscConnMap();
        Map<VolumeNumber, Volume> getVlmMap();
    }

    // Reference to the resource definition
    private final ResourceDefinition resourceDfn;

    // State flags
    private final StateFlags<Flags> flags;

    // Connections to the peer resources
    private final TransactionMap<Resource.ResourceKey, ResourceConnection> resourceConnections;
    private final TransactionMap<VolumeNumber, Volume> vlmMap;

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
        Map<VolumeNumber, Volume> vlmMapRef
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
                )
            ),
            transMgrProviderRef,
            transObjFactory
        );
        dbDriver = dbDriverRef;

        ErrorCheck.ctorNotNull(Resource.class, ResourceDefinition.class, resDfnRef);
        resourceDfn = resDfnRef;
        resourceConnections = transObjFactory.createTransactionMap(rscConnMapRef, null);
        objProt = objProtRef;
        vlmMap = transObjFactory.createTransactionMap(vlmMapRef, null);

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

    public ResourceDefinition getDefinition()
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
        getStateFlags().enableFlags(accCtx, Flags.DELETE);
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
                rootLayerData.get().delete();
            }

            activateTransMgr();
            dbDriver.delete(this);

            deleted.set(true);
        }
    }

    public void setCreatePrimary()
    {
        createPrimary = true;
    }

    public void unsetCreatePrimary()
    {
        createPrimary = false;
    }


    public boolean isCreatePrimary()
    {
        return createPrimary;
    }

    public boolean isDrbdDiskless(AccessContext accCtx) throws AccessDeniedException
    {
        return getStateFlags().isSet(accCtx, Flags.DRBD_DISKLESS);
    }

    public boolean isNvmeInitiator(AccessContext accCtx) throws AccessDeniedException
    {
        return getStateFlags().isSet(accCtx, Flags.NVME_INITIATOR);
    }

    public ResourceApi getApiData(AccessContext accCtx, Long fullSyncId, Long updateId)
        throws AccessDeniedException
    {
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
            getDefinition().getName().getDisplayName(),
            getNode().getName().getDisplayName(),
            getNode().getUuid(),
            getDefinition().getApiData(accCtx),
            getUuid(),
            getStateFlags().getFlagsBits(accCtx),
            getProps(accCtx).map(),
            volumes,
            null, // otherRscList
            rscConns,
            fullSyncId,
            updateId,
            getLayerData(accCtx).asPojo(accCtx)
        );
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
                eq = getDefinition().compareTo(((Resource) otherRsc).getDefinition());
            }
        }
        return eq;
    }

    @Override
    public String toString()
    {
        return "Node: '" + node.getName() + "', " +
               "Rsc: '" + resourceDfn.getName() + "'";
    }

    public static String getStringId(Resource rsc)
    {
        return rsc.getNode().getName().value + "/" +
            rsc.getDefinition().getName().value;
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
            this(resource.getNode().getName(), resource.getDefinition().getName());
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
        public boolean equals(Object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || getClass() != o.getClass())
            {
                return false;
            }
            ResourceKey that = (ResourceKey) o;
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

        NVME_INITIATOR(DISKLESS.flagValue | 1L << 9);

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
    }

    @Override
    public ResourceDefinition getResourceDefinition()
    {
        return resourceDfn;
    }
}
