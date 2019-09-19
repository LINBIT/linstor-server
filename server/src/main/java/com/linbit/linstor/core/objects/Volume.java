package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.linstor.AccessToDeletedDataException;
import com.linbit.linstor.DbgInstanceUuid;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.api.pojo.VlmPojo;
import com.linbit.linstor.core.apis.VolumeApi;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDatabaseDriver;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsAccess;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.interfaces.categories.resource.RscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMap;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;
import com.linbit.utils.Pair;

import javax.inject.Provider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class Volume extends BaseTransactionObject implements DbgInstanceUuid, Comparable<Volume>, LinstorDataObject
{

    public static interface InitMaps
    {
        Map<Key, VolumeConnection> getVolumeConnections();
    }

    // Object identifier
    private final UUID objId;

    // Runtime instance identifier for debug purposes
    private final transient UUID dbgInstanceId;

    // Reference to the resource this volume belongs to
    private final Resource resource;

    // Reference to the resource definition that defines the resource this volume belongs to
    private final ResourceDefinition resourceDfn;

    // Reference to the volume definition that defines this volume
    private final VolumeDefinition volumeDfn;

    // Properties container for this volume
    private final Props volumeProps;

    // State flags
    private final StateFlags<Volume.Flags> flags;

    private final TransactionMap<Volume.Key, VolumeConnection> volumeConnections;

    private final TransactionSimpleObject<Volume, String> devicePath;

    private final TransactionSimpleObject<Volume, Long> usableSize;

    private final TransactionSimpleObject<Volume, Long> allocatedSize;

    private final VolumeDatabaseDriver dbDriver;

    private final TransactionSimpleObject<Volume, Boolean> deleted;

    private final Key vlmKey;

    private ApiCallRcImpl reports;

    Volume(
        UUID uuid,
        Resource resRef,
        VolumeDefinition volDfnRef,
        long initFlags,
        VolumeDatabaseDriver dbDriverRef,
        PropsContainerFactory propsContainerFactory,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProviderRef,
        Map<Volume.Key, VolumeConnection> vlmConnsMapRef
    )
        throws DatabaseException
    {
        super(transMgrProviderRef);

        objId = uuid;
        dbgInstanceId = UUID.randomUUID();
        resource = resRef;
        resourceDfn = resRef.getDefinition();
        volumeDfn = volDfnRef;
        devicePath = transObjFactory.createTransactionSimpleObject(this, null, null);
        dbDriver = dbDriverRef;

        flags = transObjFactory.createStateFlagsImpl(
            resRef.getObjProt(),
            this,
            Volume.Flags.class,
            this.dbDriver.getStateFlagsPersistence(),
            initFlags
        );

        volumeConnections = transObjFactory.createTransactionMap(vlmConnsMapRef, null);
        volumeProps = propsContainerFactory.getInstance(
            PropsContainer.buildPath(
                resRef.getAssignedNode().getName(),
                resRef.getDefinition().getName(),
                volDfnRef.getVolumeNumber()
            )
        );
        usableSize = transObjFactory.createTransactionSimpleObject(this, null, null);
        allocatedSize = transObjFactory.createTransactionSimpleObject(this, null, null);
        deleted = transObjFactory.createTransactionSimpleObject(this, false, null);

        vlmKey = new Key(this);
        reports = new ApiCallRcImpl();

        transObjs = Arrays.asList(
            resource,
            volumeDfn,
            volumeConnections,
            volumeProps,
            usableSize,
            flags,
            deleted
        );
    }

    @Override
    public UUID debugGetVolatileUuid()
    {
        return dbgInstanceId;
    }

    public UUID getUuid()
    {
        checkDeleted();
        return objId;
    }

    public Props getProps(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        return PropsAccess.secureGetProps(accCtx, resource.getObjProt(), volumeProps);
    }

    public Resource getResource()
    {
        checkDeleted();
        return resource;
    }

    public ResourceDefinition getResourceDefinition()
    {
        checkDeleted();
        return resourceDfn;
    }

    public VolumeDefinition getVolumeDefinition()
    {
        checkDeleted();
        return volumeDfn;
    }

    public Stream<VolumeConnection> streamVolumeConnections(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        resource.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return volumeConnections.values().stream();
    }

    public VolumeConnection getVolumeConnection(AccessContext accCtx, Volume othervolume)
        throws AccessDeniedException
    {
        checkDeleted();
        resource.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return volumeConnections.get(othervolume.getKey());
    }

    public void setVolumeConnection(AccessContext accCtx, VolumeConnection volumeConnection)
        throws AccessDeniedException
    {
        checkDeleted();

        Volume sourceVolume = volumeConnection.getSourceVolume(accCtx);
        Volume targetVolume = volumeConnection.getTargetVolume(accCtx);

        sourceVolume.getResource().getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        targetVolume.getResource().getObjProt().requireAccess(accCtx, AccessType.CHANGE);

        if (this == sourceVolume)
        {
            volumeConnections.put(targetVolume.getKey(), volumeConnection);
        }
        else
        {
            volumeConnections.put(sourceVolume.getKey(), volumeConnection);
        }
    }

    public void removeVolumeConnection(AccessContext accCtx, VolumeConnection volumeConnection)
        throws AccessDeniedException
    {
        checkDeleted();

        Volume sourceVolume = volumeConnection.getSourceVolume(accCtx);
        Volume targetVolume = volumeConnection.getTargetVolume(accCtx);

        sourceVolume.getResource().getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        targetVolume.getResource().getObjProt().requireAccess(accCtx, AccessType.CHANGE);

        if (this == sourceVolume)
        {
            volumeConnections.remove(targetVolume.getKey());
        }
        else
        {
            volumeConnections.remove(sourceVolume.getKey());
        }
    }

    public StateFlags<Volume.Flags> getFlags()
    {
        checkDeleted();
        return flags;
    }

    public String getDevicePath(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        resource.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return devicePath.get();
    }

    public void setDevicePath(AccessContext accCtx, String path) throws AccessDeniedException
    {
        checkDeleted();
        resource.getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        try
        {
            devicePath.set(path);
        }
        catch (DatabaseException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    public void markDeleted(AccessContext accCtx)
        throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        resource.getObjProt().requireAccess(accCtx, AccessType.USE);
        getFlags().enableFlags(accCtx, Volume.Flags.DELETE);
    }

    public boolean isUsableSizeSet(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        resource.getObjProt().requireAccess(accCtx, AccessType.VIEW);

        return usableSize.get() != null;
    }

    public void setUsableSize(AccessContext accCtx, long size) throws AccessDeniedException
    {
        checkDeleted();
        resource.getObjProt().requireAccess(accCtx, AccessType.USE);

        try
        {
            usableSize.set(size);
        }
        catch (DatabaseException exc)
        {
            throw new ImplementationError("Driverless TransactionSimpleObject threw sql exc", exc);
        }
    }

    public long getUsableSize(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        resource.getObjProt().requireAccess(accCtx, AccessType.VIEW);

        return usableSize.get();
    }

    public boolean isAllocatedSizeSet(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        resource.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return allocatedSize.get() != null;
    }

    public void setAllocatedSize(AccessContext accCtx, long size) throws AccessDeniedException
    {
        checkDeleted();
        resource.getObjProt().requireAccess(accCtx, AccessType.USE);
        try
        {
            allocatedSize.set(size);
        }
        catch (DatabaseException exc)
        {
            throw new ImplementationError("Driverless TransactionSimpleObject threw sql exc", exc);
        }
    }

    public long getAllocatedSize(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        resource.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return allocatedSize.get();
    }

    public long getEstimatedSize(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        resource.getObjProt().requireAccess(accCtx, AccessType.VIEW);

        return volumeDfn.getVolumeSize(accCtx);
    }

    public boolean isDeleted()
    {
        return deleted.get();
    }

    public void delete(AccessContext accCtx)
        throws AccessDeniedException, DatabaseException
    {
        if (!deleted.get())
        {
            resource.getObjProt().requireAccess(accCtx, AccessType.USE);

            // preventing ConcurrentModificationException
            Collection<VolumeConnection> values = new ArrayList<>(volumeConnections.values());
            for (VolumeConnection vlmConn : values)
            {
                vlmConn.delete(accCtx);
            }

            resource.removeVolume(accCtx, this);
            ((VolumeDefinition) volumeDfn).removeVolume(accCtx, this);

            volumeProps.delete();

            activateTransMgr();
            dbDriver.delete(this);

            deleted.set(true);
        }
    }

    private void checkDeleted()
    {
        if (deleted.get())
        {
            throw new AccessToDeletedDataException("Access to deleted volume");
        }
    }

    @Override
    public String toString()
    {
        return "Node: '" + resource.getAssignedNode().getName() + "', " +
               "Rsc: '" + resource.getDefinition().getName() + "', " +
               "VlmNr: '" + volumeDfn.getVolumeNumber() + "'";
    }

    @Override
    public int compareTo(Volume otherVlm)
    {
        int eq = getResource().getAssignedNode().compareTo(
            otherVlm.getResource().getAssignedNode()
        );
        if (eq == 0)
        {
            eq = getVolumeDefinition().compareTo(otherVlm.getVolumeDefinition()); // also contains rscName comparison
        }
        return eq;
    }

    public static String getVolumeKey(Volume volume)
    {
        NodeName nodeName = volume.getResource().getAssignedNode().getName();
        ResourceName rscName = volume.getResourceDefinition().getName();
        VolumeNumber volNr = volume.getVolumeDefinition().getVolumeNumber();
        return nodeName.value + "/" + rscName.value + "/" + volNr.value;
    }

    public VolumeApi getApiData(Long allocated, AccessContext accCtx) throws AccessDeniedException
    {
        VolumeNumber vlmNr = getVolumeDefinition().getVolumeNumber();
        List<Pair<String, VlmLayerDataApi>> layerDataList = new ArrayList<>();

        StorPool compatStorPool = null;

        LinkedList<RscLayerObject> rscLayersToExpand = new LinkedList<>();
        rscLayersToExpand.add(resource.getLayerData(accCtx));
        while (!rscLayersToExpand.isEmpty())
        {
            RscLayerObject rscLayerObject = rscLayersToExpand.removeFirst();
            VlmProviderObject vlmProvider = rscLayerObject.getVlmLayerObjects().get(vlmNr);
            if (vlmProvider != null)
            {
                // vlmProvider is null is a layer (like DRBD) does not need for all volumes backing vlmProvider
                // (like in the case of mixed internal and external meta-data)
                layerDataList.add(
                    new Pair<>(
                        vlmProvider.getLayerKind().name(),
                        vlmProvider.asPojo(accCtx)
                    )
                );
            }

            // deprecated - only for compatibility with old versions
            if (rscLayerObject.getResourceNameSuffix().equals("")) // for "" resources vlmProvider always have to exist
            {
                compatStorPool = vlmProvider.getStorPool();
            }

            rscLayersToExpand.addAll(rscLayerObject.getChildren());
        }


        String compatStorPoolName = null;
        DeviceProviderKind compatStorPoolKind = null;
        if (compatStorPool != null)
        {
            compatStorPoolName = compatStorPool.getName().displayValue;
            compatStorPoolKind = compatStorPool.getDeviceProviderKind();
        }

        return new VlmPojo(
            getVolumeDefinition().getUuid(),
            getUuid(),
            getDevicePath(accCtx),
            vlmNr.value,
            getFlags().getFlagsBits(accCtx),
            getProps(accCtx).map(),
            Optional.ofNullable(allocated),
            Optional.ofNullable(usableSize.get()),
            layerDataList,
            compatStorPoolName,
            compatStorPoolKind,
            getReports()
        );
    }

    /**
     * Returns the identification key without checking if "this" is already deleted
     */
    public Key getKey()
    {
        return vlmKey;
    }

    public static enum Flags implements com.linbit.linstor.stateflags.Flags
    {
        DELETE(2L), RESIZE(4L), DRBD_RESIZE(8L);

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

        public static Flags[] restoreFlags(long vlmFlags)
        {
            List<Flags> flagList = new ArrayList<>();
            for (Flags flag : Flags.values())
            {
                if ((vlmFlags & flag.flagValue) == flag.flagValue)
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

    /**
     * Identifies a volume globally.
     */
    public static class Key implements Comparable<Key>
    {
        private final NodeName nodeName;
        private final ResourceName resourceName;
        private final VolumeNumber volumeNumber;

        public Key(Volume volume)
        {
            this(
                volume.getResource().getAssignedNode().getName(),
                volume.getResourceDefinition().getName(),
                volume.getVolumeDefinition().getVolumeNumber()
            );
        }

        public Key(NodeName nodeNameRef, ResourceName resourceNameRef, VolumeNumber volumeNumberRef)
        {
            nodeName = nodeNameRef;
            resourceName = resourceNameRef;
            volumeNumber = volumeNumberRef;
        }

        public NodeName getNodeName()
        {
            return nodeName;
        }

        public ResourceName getResourceName()
        {
            return resourceName;
        }

        public VolumeNumber getVolumeNumber()
        {
            return volumeNumber;
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
            if (o == null || getClass() != o.getClass())
            {
                return false;
            }
            Key that = (Key) o;
            return Objects.equals(nodeName, that.nodeName) &&
                Objects.equals(resourceName, that.resourceName) &&
                Objects.equals(volumeNumber, that.volumeNumber);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(nodeName, resourceName, volumeNumber);
        }

        @Override
        @SuppressWarnings("unchecked")
        public int compareTo(Key other)
        {
            int eq = nodeName.compareTo(other.nodeName);
            if (eq == 0)
            {
                eq = resourceName.compareTo(other.resourceName);
                if (eq == 0)
                {
                    eq = volumeNumber.compareTo(other.volumeNumber);
                }
            }
            return eq;
        }

        @Override
        public String toString()
        {
            return "Volume.Key [nodeName=" + nodeName + ", resourceName=" + resourceName + ", volumeNumber=" +
                volumeNumber + "]";
        }
    }

    @Override
    public ApiCallRc getReports()
    {
        return reports;
    }

    @Override
    public void addReports(ApiCallRc apiCallRc)
    {
        reports.addEntries(apiCallRc);
    }

    @Override
    public void clearReports()
    {
        reports = new ApiCallRcImpl();
    }
}
