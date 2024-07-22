package com.linbit.linstor.core.devmgr;

import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.interfaces.NodeInfo;
import com.linbit.linstor.interfaces.StorPoolInfo;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.propscon.ReadOnlyPropsImpl;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.UUID;

/**
 * A holder class that contains the necessary data for API calls like FetchFreeSpaces so that those API calls do not
 * need to take the LINSTOR locks so they can work concurrently with the DeviceManager.
 * <br/>
 * When the DeviceManager starts a new run (after the newest data was received from the Controller), a new instance of
 * this class is created.
 * <br/>
 * All fields of this class should be read-only (final and/or unmodifiable Collections)
 */
@ThreadSafe
public class StltReadOnlyInfo
{
    private final Collection<ReadOnlyStorPool> roStorPoolList;

    public StltReadOnlyInfo(Collection<ReadOnlyStorPool> roStorPoolListRef)
    {
        roStorPoolList = Collections.unmodifiableCollection(roStorPoolListRef);
    }

    public Collection<ReadOnlyStorPool> getStorPoolReadOnlyInfoList()
    {
        return roStorPoolList;
    }

    public static class ReadOnlyStorPool implements StorPoolInfo, Comparable<ReadOnlyStorPool>
    {
        private final UUID uuid;
        private final StorPoolName name;
        private final SharedStorPoolName sharedStorPoolName; // required by space-tracking
        private final DeviceProviderKind devProviderKind;
        private final ReadOnlyNode node;
        private final ReadOnlyProps roProps;

        private final Collection<ReadOnlyVlmProviderInfo> roVolumes;

        public static ReadOnlyStorPool copyFrom(
            @Nullable ReadOnlyNode roNodeRef,
            StorPool storPoolRef,
            AccessContext accCtxRef
        )
            throws AccessDeniedException
        {
            return new ReadOnlyStorPool(
                storPoolRef.getUuid(),
                storPoolRef.getName(),
                storPoolRef.getSharedStorPoolName(),
                storPoolRef.getDeviceProviderKind(),
                roNodeRef == null ? ReadOnlyNode.copyFrom(storPoolRef.getNode(), accCtxRef) : roNodeRef,
                storPoolRef.getReadOnlyProps(accCtxRef),
                storPoolRef.getVolumes(accCtxRef)
            );
        }

        public ReadOnlyStorPool(
            UUID uuidRef,
            StorPoolName nameRef,
            SharedStorPoolName sharedStorPoolNameRef,
            DeviceProviderKind devProviderKindRef,
            ReadOnlyNode nodeRef,
            ReadOnlyProps roPropsRef,
            Collection<VlmProviderObject<Resource>> vlmProviderObjListRef
        )
        {
            uuid = uuidRef;
            name = nameRef;
            sharedStorPoolName = sharedStorPoolNameRef;
            devProviderKind = devProviderKindRef;
            node = nodeRef;
            roProps = roPropsRef;

            ArrayList<ReadOnlyVlmProviderInfo> roVlmProvInfoList = new ArrayList<>();
            for (VlmProviderObject<Resource> vlmProvObj : vlmProviderObjListRef)
            {
                roVlmProvInfoList.add(new ReadOnlyVlmProviderInfo(vlmProvObj, this));
            }
            roVolumes = Collections.unmodifiableCollection(roVlmProvInfoList);
        }

        @Override
        public DeviceProviderKind getDeviceProviderKind()
        {
            return devProviderKind;
        }

        @Override
        public StorPoolName getName()
        {
            return name;
        }

        @Override
        public SharedStorPoolName getSharedStorPoolName()
        {
            // required by space-tracking
            return sharedStorPoolName;
        }

        @Override
        public UUID getUuid()
        {
            return uuid;
        }

        @Override
        public ReadOnlyNode getReadOnlyNode()
        {
            return node;
        }

        @Override
        public ReadOnlyProps getReadOnlyProps(AccessContext accCtxRef)
        {
            return roProps;
        }

        public Collection<ReadOnlyVlmProviderInfo> getReadOnlyVolumes()
        {
            return roVolumes;
        }

        @Override
        public int compareTo(ReadOnlyStorPool otherRef)
        {
            int eq = node.compareTo(otherRef.node);
            if (eq == 0)
            {
                eq = name.compareTo(otherRef.name);
            }
            return eq;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name, node);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
            {
                return true;
            }
            if (!(obj instanceof ReadOnlyStorPool))
            {
                return false;
            }
            ReadOnlyStorPool other = (ReadOnlyStorPool) obj;
            return Objects.equals(name, other.name) && Objects.equals(node, other.node);
        }
    }

    public static class ReadOnlyNode implements NodeInfo, Comparable<ReadOnlyNode>
    {
        private final UUID uuid;
        private final NodeName name;
        private final ReadOnlyProps props;

        public static ReadOnlyNode copyFrom(Node nodeRef, AccessContext accCtxRef) throws AccessDeniedException
        {
            return new ReadOnlyNode(
                nodeRef.getUuid(),
                nodeRef.getName(),
                new ReadOnlyPropsImpl(nodeRef.getProps(accCtxRef))
            );
        }

        public ReadOnlyNode(UUID uuidRef, NodeName nameRef, ReadOnlyProps propsRef)
        {
            uuid = uuidRef;
            name = nameRef;
            props = propsRef;
        }

        @Override
        public UUID getUuid()
        {
            return uuid;
        }

        @Override
        public NodeName getName()
        {
            return name;
        }

        @Override
        public ReadOnlyProps getReadOnlyProps(AccessContext accCtxRef)
        {
            return props;
        }

        @Override
        public int compareTo(ReadOnlyNode otherRef)
        {
            return name.compareTo(otherRef.name);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name, props);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
            {
                return true;
            }
            if (!(obj instanceof ReadOnlyNode))
            {
                return false;
            }
            ReadOnlyNode other = (ReadOnlyNode) obj;
            return Objects.equals(name, other.name) && Objects.equals(props, other.props);
        }
    }

    public static class ReadOnlyVlmProviderInfo
    {
        private final ResourceName rscName;
        private final String rscSuffix;
        private final VolumeNumber vlmNr;
        private final ReadOnlyStorPool roStorPool;
        /** Null for {@link SnapshotVolume}s */
        private final @Nullable Volume.Key vlmKey;
        /** Null for {@link Volume}s */
        private final @Nullable SnapshotName snapName;
        /** Null for {@link Volume}s */
        private final @Nullable SnapshotVolume.Key snapVlmKey;

        /** Possibly null for {@link SnapshotVolume}s or {@link Volume}s that have not been initialized yet */
        private final @Nullable String devicePath;
        /** Possibly null if the origin instance has not been initialized yet */
        private final @Nullable String identifier;
        /**
         * Possibly {@link VlmProviderObject#UNINITIALIZED_SIZE} for {@link SnapshotVolume}s or {@link Volume}s that
         * have not been initialized yet
         */
        private final long origAllocatedSize;

        public ReadOnlyVlmProviderInfo(VlmProviderObject<?> vlmProvObjRef, ReadOnlyStorPool roStorPoolRef)
        {
            AbsRscLayerObject<?> absRscLayerObject = vlmProvObjRef.getRscLayerObject();
            AbsResource<?> absResource = absRscLayerObject.getAbsResource();

            rscName = absRscLayerObject.getResourceName();
            rscSuffix = absRscLayerObject.getResourceNameSuffix();
            vlmNr = vlmProvObjRef.getVlmNr();
            if (absResource instanceof Snapshot)
            {
                vlmKey = null;
                snapName = ((Snapshot) absResource).getSnapshotName();
                snapVlmKey = ((SnapshotVolume) vlmProvObjRef.getVolume()).getKey();
            }
            else
            {
                vlmKey = ((Volume) vlmProvObjRef.getVolume()).getKey();
                snapName = null;
                snapVlmKey = null;
            }
            roStorPool = roStorPoolRef;
            devicePath = vlmProvObjRef.getDevicePath();
            origAllocatedSize = vlmProvObjRef.getAllocatedSize();
            identifier = vlmProvObjRef.getIdentifier();
        }

        public ResourceName getResourceName()
        {
            return rscName;
        }

        public String getRscSuffix()
        {
            return rscSuffix;
        }

        public VolumeNumber getVlmNr()
        {
            return vlmNr;
        }

        public ReadOnlyStorPool getReadOnlyStorPool()
        {
            return roStorPool;
        }

        /**
         * @return The {@link SnapshotName} if this instance was initialized using a {@link SnapshotVolume}. Otherwise
         * this method returns null.
         */
        public @Nullable SnapshotName getSnapName() {
            return snapName;
        }

        /**
         * @return the {@link com.linbit.linstor.core.objects.Volume.Key} if this instance was initialized with
         * a {@link Volume}. If it was initialized with a {@link SnapshotVolume}, this method returns null. In that
         * case, use {@link #getSnapVolumeKey()} instead.
         */
        public @Nullable Volume.Key getVolumeKey()
        {
            return vlmKey;
        }

        /**
         * @return the {@link com.linbit.linstor.core.objects.SnapshotVolume.Key} if this instance was initialized with
         * a {@link SnapshotVolume}. If it was initialized with a {@link Volume}, this method returns null. In that
         * case, use {@link #getVolumeKey()} instead.
         */
        public @Nullable SnapshotVolume.Key getSnapVolumeKey()
        {
            return snapVlmKey;
        }

        /**
         * Possibly null for {@link SnapshotVolume}s or {@link Volume}s that have not been initialized yet
         */
        public @Nullable String getDevicePath()
        {
            return devicePath;
        }

        /**
         * Possibly {@link VlmProviderObject#UNINITIALIZED_SIZE} for {@link SnapshotVolume}s or {@link Volume}s that
         * have not been initialized yet. This method can be used for thick-implementations.
         */
        public long getOrigAllocatedSize()
        {
            return origAllocatedSize;
        }

        /**
         * Possibly null if the origin instance has not been initialized yet
         */
        public String getIdentifier()
        {
            return identifier;
        }
    }
}
