package com.linbit.linstor.core.objects;

import com.linbit.ErrorCheck;
import com.linbit.ImplementationError;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.pojo.StorPoolPojo;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apis.StorPoolApi;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDatabaseDriver;
import com.linbit.linstor.interfaces.NodeInfo;
import com.linbit.linstor.interfaces.StorPoolInfo;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsAccess;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.propscon.ReadOnlyPropsImpl;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ProtectedObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.TransactionMap;
import com.linbit.linstor.transaction.TransactionObject;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import static com.linbit.linstor.api.ApiConsts.KEY_STOR_POOL_SUPPORTS_SNAPSHOTS;

import javax.inject.Provider;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

public class StorPool extends AbsCoreObj<StorPool>
    implements LinstorDataObject, ProtectedObject, StorPoolInfo
{
    public interface InitMaps
    {
        Map<String, VlmProviderObject<Resource>> getVolumeMap();

        Map<String, VlmProviderObject<Snapshot>> getSnapshotVolumeMap();
    }

    private final StorPoolDefinition storPoolDef;
    private final DeviceProviderKind deviceProviderKind;

    private final Props props;
    private final ReadOnlyProps roProps;
    private final Node node;
    private final StorPoolDatabaseDriver dbDriver;
    private final FreeSpaceTracker freeSpaceTracker;
    private final boolean externalLocking;
    private final Key storPoolKey;

    private final TransactionMap<StorPool, String, VlmProviderObject<Resource>> vlmProviderMap;
    private final TransactionMap<StorPool, String, VlmProviderObject<Snapshot>> snapVlmProviderMap;

    /**
     * This boolean is only asked if the deviceProviderKind is FILE or FILE_THIN. Otherwise
     * the query is forwarded to the deviceProviderKind
     */
    private final TransactionSimpleObject<StorPool, Boolean> supportsSnapshots;

    private final TransactionSimpleObject<StorPool, Boolean> isPmem;

    private ApiCallRcImpl reports;


    StorPool(
        UUID id,
        Node nodeRef,
        StorPoolDefinition storPoolDefRef,
        DeviceProviderKind providerKindRef,
        FreeSpaceTracker freeSpaceTrackerRef,
        boolean externalLockingRef,
        StorPoolDatabaseDriver dbDriverRef,
        PropsContainerFactory propsContainerFactory,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProviderRef,
        Map<String, VlmProviderObject<Resource>> volumeMapRef,
        Map<String, VlmProviderObject<Snapshot>> snapshotVolumeMapRef
    )
        throws DatabaseException
    {
        super(id, transObjFactory, transMgrProviderRef);
        ErrorCheck.ctorNotNull(StorPool.class, FreeSpaceTracker.class, freeSpaceTrackerRef);

        storPoolDef = storPoolDefRef;
        deviceProviderKind = providerKindRef;
        freeSpaceTracker = freeSpaceTrackerRef;
        node = nodeRef;
        dbDriver = dbDriverRef;
        externalLocking = externalLockingRef;
        vlmProviderMap = transObjFactory.createTransactionMap(this, volumeMapRef, null);
        snapVlmProviderMap = transObjFactory.createTransactionMap(this, snapshotVolumeMapRef, null);
        storPoolKey = new Key(this);

        props = propsContainerFactory.getInstance(
            PropsContainer.buildPath(storPoolDef.getName(), node.getName()),
            toStringImpl(),
            LinStorObject.STOR_POOL
        );
        roProps = new ReadOnlyPropsImpl(props);

        final boolean isFileProviderKind = providerKindRef == DeviceProviderKind.FILE ||
            providerKindRef == DeviceProviderKind.FILE_THIN;

        supportsSnapshots = transObjFactory.<StorPool, Boolean>createTransactionSimpleObject(
            this,
            isFileProviderKind ? null : providerKindRef.isSnapshotSupported(),
            null
        );
        isPmem = transObjFactory.createTransactionSimpleObject(this, false, null);

        reports = new ApiCallRcImpl();

        transObjs = Arrays.<TransactionObject>asList(
            vlmProviderMap,
            snapVlmProviderMap,
            props,
            deleted,
            freeSpaceTracker
        );
        activateTransMgr();
    }

    @Override
    public StorPoolName getName()
    {
        checkDeleted();
        return storPoolDef.getName();
    }

    public Node getNode()
    {
        checkDeleted();
        return node;
    }

    @Override
    public NodeInfo getReadOnlyNode()
    {
        checkDeleted();
        return node;
    }

    public StorPoolDefinition getDefinition(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        node.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        storPoolDef.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return storPoolDef;
    }

    @Override
    public DeviceProviderKind getDeviceProviderKind()
    {
        checkDeleted();
        return deviceProviderKind;
    }

    public Props getProps(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        return PropsAccess.secureGetProps(accCtx, node.getObjProt(), storPoolDef.getObjProt(), props);
    }

    @Override
    public ReadOnlyProps getReadOnlyProps(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        node.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        storPoolDef.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return roProps;
    }

    public void putVolume(AccessContext accCtx, VlmProviderObject<Resource> vlmProviderObj)
        throws AccessDeniedException
    {
        checkDeleted();
        node.getObjProt().requireAccess(accCtx, AccessType.USE);
        storPoolDef.getObjProt().requireAccess(accCtx, AccessType.USE);

        vlmProviderMap.put(vlmProviderObj.getVolumeKey(), vlmProviderObj);
        freeSpaceTracker.vlmCreating(accCtx, vlmProviderObj);
    }

    public void removeVolume(AccessContext accCtx, VlmProviderObject<Resource> vlmProviderObj)
        throws AccessDeniedException
    {
        checkDeleted();
        node.getObjProt().requireAccess(accCtx, AccessType.USE);
        storPoolDef.getObjProt().requireAccess(accCtx, AccessType.USE);

        freeSpaceTracker.ensureVlmNoLongerCreating(accCtx, vlmProviderObj);

        vlmProviderMap.remove(vlmProviderObj.getVolumeKey());
    }

    public Collection<VlmProviderObject<Resource>> getVolumes(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        node.getObjProt().requireAccess(accCtx, AccessType.USE);
        storPoolDef.getObjProt().requireAccess(accCtx, AccessType.USE);

        return vlmProviderMap.values();
    }

    public boolean isPmem()
    {
        checkDeleted();
        return isPmem.get();
    }

    public void setPmem(boolean pmemRef) throws DatabaseException
    {
        checkDeleted();
        isPmem.set(pmemRef);
    }

    public void putSnapshotVolume(AccessContext accCtx, VlmProviderObject<Snapshot> vlmProviderObj)
        throws AccessDeniedException
    {
        checkDeleted();
        node.getObjProt().requireAccess(accCtx, AccessType.USE);
        storPoolDef.getObjProt().requireAccess(accCtx, AccessType.USE);

        snapVlmProviderMap.put(vlmProviderObj.getVolumeKey(), vlmProviderObj);
        freeSpaceTracker.vlmCreating(accCtx, vlmProviderObj);
    }

    public void removeSnapshotVolume(AccessContext accCtx, VlmProviderObject<Snapshot> vlmProviderObj)
        throws AccessDeniedException
    {
        checkDeleted();
        node.getObjProt().requireAccess(accCtx, AccessType.USE);
        storPoolDef.getObjProt().requireAccess(accCtx, AccessType.USE);

        freeSpaceTracker.ensureVlmNoLongerCreating(accCtx, vlmProviderObj);

        snapVlmProviderMap.remove(vlmProviderObj.getVolumeKey());
    }

    public Collection<VlmProviderObject<Snapshot>> getSnapVolumes(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        node.getObjProt().requireAccess(accCtx, AccessType.USE);
        storPoolDef.getObjProt().requireAccess(accCtx, AccessType.USE);

        return snapVlmProviderMap.values();
    }

    public FreeSpaceTracker getFreeSpaceTracker()
    {
        checkDeleted();
        return freeSpaceTracker;
    }

    @Override
    public void delete(AccessContext accCtx)
        throws AccessDeniedException, DatabaseException
    {
        if (!deleted.get())
        {
            node.getObjProt().requireAccess(accCtx, AccessType.USE);
            storPoolDef.getObjProt().requireAccess(accCtx, AccessType.USE);

            node.removeStorPool(accCtx, this);
            storPoolDef.removeStorPool(accCtx, this);

            props.delete();

            activateTransMgr();
            dbDriver.delete(this);

            deleted.set(true);
        }
    }

    @Override
    public ApiCallRc getReports()
    {
        final ApiCallRcImpl localReports = reports;
        synchronized (localReports)
        {
            return new ApiCallRcImpl(localReports);
        }
    }

    @Override
    public void addReports(ApiCallRc apiCallRc)
    {
        final ApiCallRcImpl localReports = reports;
        synchronized (localReports)
        {
            localReports.addEntries(apiCallRc);
        }
    }

    @Override
    public void clearReports()
    {
        reports = new ApiCallRcImpl();
    }

    public void setSupportsSnapshot(AccessContext accCtx, boolean supportsSnapshotsRef)
        throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        node.getObjProt().requireAccess(accCtx, AccessType.USE);
        storPoolDef.getObjProt().requireAccess(accCtx, AccessType.USE);

        supportsSnapshots.set(supportsSnapshotsRef);
    }

    public boolean isSnapshotSupported(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        node.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        storPoolDef.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        boolean ret;
        if (deviceProviderKind.equals(DeviceProviderKind.FILE) ||
            deviceProviderKind.equals(DeviceProviderKind.FILE_THIN))
        {
            ret = supportsSnapshots.get();
        }
        else
        {
            ret = deviceProviderKind.isSnapshotSupported();
        }
        return ret;
    }

    public boolean isSnapshotSupportedInitialized(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        node.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        storPoolDef.getObjProt().requireAccess(accCtx, AccessType.VIEW);

        return supportsSnapshots.get() != null;
    }

    public boolean isExternalLocking()
    {
        checkDeleted();
        return externalLocking;
    }

    public boolean isShared()
    {
        checkDeleted();
        return freeSpaceTracker.getName().isShared() && !externalLocking;
    }

    @Override
    public SharedStorPoolName getSharedStorPoolName()
    {
        checkDeleted();
        return freeSpaceTracker.getName();
    }

    private Map<String, String> getTraits(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        node.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        storPoolDef.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        Map<String, String> traits = new HashMap<>(deviceProviderKind.getStorageDriverKind().getStaticTraits());

        traits.put(
            KEY_STOR_POOL_SUPPORTS_SNAPSHOTS,
            String.valueOf(deviceProviderKind.getStorageDriverKind().isSnapshotSupported())
        );

        return traits;
    }

    public double getOversubscriptionRatio(AccessContext accCtxRef, @Nullable ReadOnlyProps ctrlPropsRef)
        throws AccessDeniedException
    {
        Double override = null; // override, regardless of property
        Double dfltVal = null; // use value if property is missing
        switch (deviceProviderKind)
        {
            case DISKLESS:
            case EBS_INIT:
                override = Double.POSITIVE_INFINITY;
                break;
            case EBS_TARGET:
            case FILE:
            case LVM:
            case REMOTE_SPDK:
            case SPDK:
            case ZFS:
            case STORAGE_SPACES:
                dfltVal = 1.0;
                break;
            case FILE_THIN:
            case LVM_THIN:
            case ZFS_THIN:
            case STORAGE_SPACES_THIN:
                dfltVal = LinStor.OVERSUBSCRIPTION_RATIO_DEFAULT;
                break;
            case FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER:
            default:
                throw new ImplementationError("Unexpected device prodivder kind: " + deviceProviderKind);
        }
        double ret;
        if (override != null)
        {
            ret = override;
        }
        else
        {
            // maybe also extend with ctrl-props, but we would need that as
            // argument, which is weird...
            String oversubscriptionProp = new PriorityProps(
                props,
                getDefinition(accCtxRef).getProps(accCtxRef),
                ctrlPropsRef
            )
                .getProp(ApiConsts.KEY_STOR_POOL_MAX_OVERSUBSCRIPTION_RATIO);
            if (oversubscriptionProp == null)
            {
                if (dfltVal != null)
                {
                    ret = dfltVal;
                }
                else
                {
                    ret = 1.0; // no oversubscription, capacity is max
                }
            }
            else
            {
                ret = Double.parseDouble(oversubscriptionProp);
            }
        }
        return ret;
    }

    public Key getKey()
    {
        // no check-deleted
        return storPoolKey;
    }

    @Override
    public String toStringImpl()
    {
        return "Node: '" + storPoolKey.nodeName + "', " +
            "StorPool: '" + storPoolKey.storPoolName + "'";
    }

    @Override
    public int compareTo(StorPool otherStorPool)
    {
        int eq = getNode().compareTo(otherStorPool.getNode());
        if (eq == 0)
        {
            eq = getName().compareTo(otherStorPool.getName());
        }
        return eq;
    }

    @Override
    public int hashCode()
    {
        checkDeleted();
        return Objects.hash(node, storPoolDef);
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
        else if (obj instanceof StorPool)
        {
            StorPool other = (StorPool) obj;
            other.checkDeleted();
            ret = Objects.equals(node, other.node) && Objects.equals(storPoolDef, other.storPoolDef);
        }
        return ret;
    }

    public StorPoolApi getApiData(
        @Nullable Long totalSpaceRef,
        @Nullable Long freeSpaceRef,
        AccessContext accCtx,
        @Nullable Long fullSyncId,
        @Nullable Long updateId,
        @Nullable Double maxFreeCapacityOversubscriptionRatioRef,
        @Nullable Double maxTotalCapacityOversubscriptionRatioRef
    )
        throws AccessDeniedException
    {
        checkDeleted();
        return new StorPoolPojo(
            getUuid(),
            getNode().getUuid(),
            node.getName().getDisplayName(),
            getName().getDisplayName(),
            getDefinition(accCtx).getUuid(),
            getDeviceProviderKind(),
            getProps(accCtx).cloneMap(),
            getDefinition(accCtx).getProps(accCtx).cloneMap(),
            getTraits(accCtx),
            fullSyncId,
            updateId,
            getFreeSpaceTracker().getName().displayValue,
            Optional.ofNullable(freeSpaceRef),
            Optional.ofNullable(totalSpaceRef),
            getOversubscriptionRatio(accCtx, null),
            maxFreeCapacityOversubscriptionRatioRef,
            maxTotalCapacityOversubscriptionRatioRef,
            getReports(),
            isPmem.get(),
            externalLocking
        );
    }

    /**
     * Identifies a stor pool.
     */
    public static class Key implements Comparable<Key>
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
            Key that = (Key) obj;
            return Objects.equals(nodeName, that.nodeName) &&
                Objects.equals(storPoolName, that.storPoolName);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(nodeName, storPoolName);
        }

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

    @Override
    public ObjectProtection getObjProt()
    {
        return node.getObjProt();
    }
}
