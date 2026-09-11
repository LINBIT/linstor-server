package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.api.pojo.VlmPojo;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.apis.VolumeApi;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDatabaseDriver;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.TransactionMap;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.utils.PairNonNull;

import jakarta.inject.Provider;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

public class Volume extends AbsVolume<Resource>
{
    public interface InitMaps
    {
        Map<Key, VolumeConnection> getVolumeConnections();
    }

    // Reference to the volume definition that defines this volume
    private final VolumeDefinition volumeDfn;

    // Properties container for this volume
    private final Props props;

    // State flags
    private final StateFlags<Volume.Flags> flags;

    private final TransactionMap<Volume, Volume.Key, VolumeConnection> volumeConnections;

    private final TransactionSimpleObject<Volume, String> devicePath;

    private final TransactionSimpleObject<Volume, Long> usableSize;

    private final TransactionSimpleObject<Volume, Long> allocatedSize;

    private final VolumeDatabaseDriver dbDriver;

    private final Key vlmKey;

    private ApiCallRcImpl reports;

    Volume(
        UUID uuid,
        Resource rscRef,
        VolumeDefinition vlmDfnRef,
        long initFlags,
        VolumeDatabaseDriver dbDriverRef,
        Map<Volume.Key, VolumeConnection> vlmConnsMapRef,
        PropsContainerFactory propsContainerFactory,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProviderRef
    )
        throws DatabaseException
    {
        super(
            uuid,
            rscRef,
            transObjFactory,
            transMgrProviderRef
        );
        volumeDfn = vlmDfnRef;
        devicePath = transObjFactory.createTransactionSimpleObject(this, null, null);
        dbDriver = dbDriverRef;

        usableSize = transObjFactory.createTransactionSimpleObject(this, null, null);
        allocatedSize = transObjFactory.createTransactionSimpleObject(this, null, null);

        vlmKey = new Key(this);
        reports = new ApiCallRcImpl();

        props = propsContainerFactory.getInstance(
            PropsContainer.buildPath(
                rscRef.getNode().getName(),
                rscRef.getResourceDefinition().getName(),
                vlmDfnRef.getVolumeNumber()
            ),
            toStringImpl(),
            LinStorObject.VLM
        );

        flags = transObjFactory.createStateFlagsImpl(
            this,
            Flags.class,
            this.dbDriver.getStateFlagsPersistence(),
            initFlags
        );

        volumeConnections = transObjFactory.createTransactionMap(this, vlmConnsMapRef, null);

        transObjs.addAll(
            Arrays.asList(
                volumeDfn,
                volumeConnections,
                usableSize,
                flags,
                props
            )
        );
    }

    @Override
    public VolumeDefinition getVolumeDefinition()
    {
        checkDeleted();
        return volumeDfn;
    }

    public Stream<VolumeConnection> streamVolumeConnections()
    {
        checkDeleted();
        return volumeConnections.values().stream();
    }

    public @Nullable VolumeConnection getVolumeConnection(Volume othervolume)
    {
        checkDeleted();
        return volumeConnections.get(othervolume.getKey());
    }

    public void setVolumeConnection(VolumeConnection volumeConnection)
    {
        checkDeleted();

        Volume sourceVolume = volumeConnection.getSourceVolume();
        Volume targetVolume = volumeConnection.getTargetVolume();


        if (this.equals(sourceVolume))
        {
            volumeConnections.put(targetVolume.getKey(), volumeConnection);
        }
        else
        {
            volumeConnections.put(sourceVolume.getKey(), volumeConnection);
        }
    }

    public void removeVolumeConnection(VolumeConnection volumeConnection)
    {
        checkDeleted();

        Volume sourceVolume = volumeConnection.getSourceVolume();
        Volume targetVolume = volumeConnection.getTargetVolume();


        if (this.equals(sourceVolume))
        {
            volumeConnections.remove(targetVolume.getKey());
        }
        else
        {
            volumeConnections.remove(sourceVolume.getKey());
        }
    }

    public Props getProps()
    {
        checkDeleted();
        return props;
    }

    public StateFlags<Volume.Flags> getFlags()
    {
        checkDeleted();
        return flags;
    }

    public String getDevicePath()
    {
        checkDeleted();
        return devicePath.get();
    }

    public void setDevicePath(String path)
    {
        checkDeleted();
        try
        {
            devicePath.set(path);
        }
        catch (DatabaseException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    public void markDeleted()
        throws DatabaseException
    {
        checkDeleted();
        getFlags().enableFlags(Flags.DELETE);
    }

    public void markDrbdDeleted()
        throws DatabaseException
    {
        checkDeleted();
        getFlags().enableFlags(Flags.DRBD_DELETE);
    }

    public boolean isUsableSizeSet()
    {
        checkDeleted();

        return usableSize.get() != null;
    }

    public void setUsableSize(long size)
    {
        checkDeleted();

        try
        {
            usableSize.set(size);
        }
        catch (DatabaseException exc)
        {
            throw new ImplementationError("Driverless TransactionSimpleObject threw sql exc", exc);
        }
    }

    public long getUsableSize()
    {
        checkDeleted();

        return usableSize.get();
    }

    public boolean isAllocatedSizeSet()
    {
        checkDeleted();
        return allocatedSize.get() != null;
    }

    public void setAllocatedSize(long size)
    {
        checkDeleted();
        try
        {
            allocatedSize.set(size);
        }
        catch (DatabaseException exc)
        {
            throw new ImplementationError("Driverless TransactionSimpleObject threw sql exc", exc);
        }
    }

    public long getAllocatedSize()
    {
        checkDeleted();
        return allocatedSize.get();
    }

    public long getEstimatedSize()
    {
        checkDeleted();

        return volumeDfn.getVolumeSize();
    }

    @Override
    public void delete()
        throws DatabaseException
    {
        if (!deleted.get())
        {

            // preventing ConcurrentModificationException
            Collection<VolumeConnection> values = new ArrayList<>(volumeConnections.values());
            for (VolumeConnection vlmConn : values)
            {
                vlmConn.delete();
            }

            absRsc.removeVolume(this);
            volumeDfn.removeVolume(this);

            props.delete();

            activateTransMgr();
            dbDriver.delete(this);

            deleted.set(true);
        }
    }

    @Override
    public String toStringImpl()
    {
        return String.format("Node: '%s', Rsc: '%s', VlmNr: '%s'", vlmKey.nodeName, vlmKey.resourceName, vlmKey.volumeNumber);
    }

    @Override
    public int compareTo(AbsVolume<Resource> otherVlm)
    {
        int eq = 1;
        if (otherVlm instanceof Volume volume)
        {
            eq = getAbsResource().getNode().compareTo(
                otherVlm.getAbsResource().getNode()
            );
            if (eq == 0)
            {
                eq = volumeDfn.compareTo(volume.volumeDfn); // also contains rscName comparison
            }
        }
        return eq;
    }

    @Override
    public int hashCode()
    {
        checkDeleted();
        return Objects.hash(vlmKey);
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
        else if (obj instanceof Volume other)
        {
            other.checkDeleted();
            ret = Objects.equals(vlmKey, other.vlmKey);
        }
        return ret;
    }

    @Override
    public VolumeNumber getVolumeNumber()
    {
        checkDeleted();
        return volumeDfn.getVolumeNumber();
    }

    @Override
    public long getVolumeSize()
    {
        checkDeleted();
        return volumeDfn.getVolumeSize();
    }

    public static String getVolumeKey(Volume volume)
    {
        NodeName nodeName = volume.absRsc.node.getName();
        ResourceName rscName = volume.getResourceDefinition().getName();
        VolumeNumber volNr = volume.volumeDfn.getVolumeNumber();
        return nodeName.value + "/" + rscName.value + "/" + volNr.value;
    }

    public VolumeApi getApiData(@Nullable Long allocated)
    {
        checkDeleted();
        VolumeNumber vlmNr = volumeDfn.getVolumeNumber();
        List<PairNonNull<String, VlmLayerDataApi>> layerDataList = new ArrayList<>();

        StorPool compatStorPool = null;

        Deque<AbsRscLayerObject<Resource>> rscLayersToExpand = new ArrayDeque<>();
        rscLayersToExpand.add(absRsc.getLayerData());
        while (!rscLayersToExpand.isEmpty())
        {
            AbsRscLayerObject<Resource> rscLayerObject = rscLayersToExpand.removeFirst();
            VlmProviderObject<Resource> vlmProvider = rscLayerObject.getVlmLayerObjects().get(vlmNr);

            if (vlmProvider != null)
            {
                // vlmProvider is null as a layer (like DRBD) does not need for all volumes backing vlmProvider
                // (like in the case of mixed internal and external meta-data)
                layerDataList.add(
                    new PairNonNull<>(
                        vlmProvider.getLayerKind().name(),
                        vlmProvider.asPojo()
                    )
                );

                // deprecated - only for compatibility with old versions
                if (compatStorPool == null && rscLayerObject.getResourceNameSuffix().isEmpty())
                {
                    compatStorPool = vlmProvider.getStorPool();
                }
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
            volumeDfn.getUuid(),
            getUuid(),
            getDevicePath(),
            vlmNr.value,
            getFlags().getFlagsBits(),
            getProps().cloneMap(),
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

    public enum Flags implements com.linbit.linstor.stateflags.Flags
    {
        DELETE(1L << 1),
        RESIZE(1L << 2),
        DRBD_RESIZE(1L << 3),
        CLONING_START(1L << 4),
        CLONING(1L << 5),
        CLONING_FINISHED(1L << 6),
        DRBD_DELETE(1L << 7);

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
                volume.getAbsResource().getNode().getName(),
                volume.getResourceDefinition().getName(),
                volume.volumeDfn.getVolumeNumber()
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
            if (!(o instanceof Key that))
            {
                return false;
            }
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
