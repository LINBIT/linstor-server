package com.linbit.linstor.core.objects;

import com.linbit.linstor.DbgInstanceUuid;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.TransactionObject;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface StorPool extends TransactionObject, DbgInstanceUuid, Comparable<StorPool>, LinstorDataObject
{
    /**
     * Returns the {@link UUID} of this object.
     */
    UUID getUuid();

    /**
     * Returns the {@link com.linbit.linstor.StorPoolName}. This call is the same as
     * <code>getDefinition(accCtx).getName()</code> but does not require special access.
     */
    StorPoolName getName();

    /**
     * Returns the {@link com.linbit.linstor.core.objects.Node} this StorPool is associated with.
     */
    Node getNode();

    /**
     * Returns the {@link com.linbit.linstor.core.objects.StorPoolDefinition}.
     */
    StorPoolDefinition getDefinition(AccessContext accCtx)
        throws AccessDeniedException;

    DeviceProviderKind getDeviceProviderKind();

    /**
     * Returns the configuration {@link Props}. This {@link Props} is also used to configure the {@link StorageDriver}.
     */
    Props getProps(AccessContext accCtx)
        throws AccessDeniedException;

    /**
     * Registers the volume to this storPool
     */
    void putVolume(AccessContext accCtx, VlmProviderObject vlmProviderObject)
        throws AccessDeniedException;

    /**
     * Removes the volume from this storPool
     */
    void removeVolume(AccessContext accCtx, VlmProviderObject vlmProviderObject)
        throws AccessDeniedException;

    /**
     * Returns all currently registered volumes
     */
    Collection<VlmProviderObject> getVolumes(AccessContext accCtx)
        throws AccessDeniedException;

    void delete(AccessContext accCtx)
        throws AccessDeniedException, DatabaseException;

    boolean isSnapshotSupported(AccessContext accCtxRef) throws AccessDeniedException;

    StorPoolApi getApiData(
        Long totalSpace,
        Long freeSpace,
        AccessContext accCtx,
        Long fullSyncId,
        Long updateId
    )
        throws AccessDeniedException;

    FreeSpaceTracker getFreeSpaceTracker();

    @Override
    default int compareTo(StorPool otherStorPool)
    {
        int eq = getNode().compareTo(otherStorPool.getNode());
        if (eq == 0)
        {
            eq = getName().compareTo(otherStorPool.getName());
        }
        return eq;
    }

    interface StorPoolApi
    {
        UUID getStorPoolUuid();
        String getStorPoolName();
        UUID getStorPoolDfnUuid();
        String getNodeName();
        UUID getNodeUuid();
        DeviceProviderKind getDeviceProviderKind();
        String getFreeSpaceManagerName();
        Optional<Long> getFreeCapacity();
        Optional<Long> getTotalCapacity();

        Map<String, String> getStorPoolProps();
        Map<String, String> getStorPoolStaticTraits();
        Map<String, String> getStorPoolDfnProps();
        ApiCallRc getReports();
    }

    /**
     * Identifies a stor pool.
     */
    class Key implements Comparable<Key>
    {
        private final NodeName nodeName;

        private final StorPoolName storPoolName;

        public Key(NodeName nodeNameRef, StorPoolName storPoolNameRef)
        {
            nodeName = nodeNameRef;
            storPoolName = storPoolNameRef;
        }

        public Key(StorPool storPool)
        {
            this(storPool.getNode().getName(), storPool.getName());
        }

        public NodeName getNodeName()
        {
            return nodeName;
        }

        public StorPoolName getStorPoolName()
        {
            return storPoolName;
        }

        @Override
        // Code style exception: Automatically generated code
        @SuppressWarnings({"DescendantToken", "ParameterName"})
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
                Objects.equals(storPoolName, that.storPoolName);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(nodeName, storPoolName);
        }

        @SuppressWarnings("unchecked")
        @Override
        public int compareTo(Key other)
        {
            int eq = nodeName.compareTo(other.nodeName);
            if (eq == 0)
            {
                eq = storPoolName.compareTo(other.storPoolName);
            }
            return eq;
        }

        @Override
        public String toString()
        {
            return "Node: '" + nodeName + "' StorPool: '" + storPoolName + "'";
        }
    }

    public interface InitMaps
    {
        Map<String, VlmProviderObject> getVolumeMap();
    }

}
