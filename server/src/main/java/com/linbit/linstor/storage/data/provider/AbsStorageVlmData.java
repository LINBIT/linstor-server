package com.linbit.linstor.storage.data.provider;

import com.linbit.ImplementationError;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.AbsVolume;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.StorageLayerDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.interfaces.layers.State;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionList;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public abstract class AbsStorageVlmData<RSC extends AbsResource<RSC>>
    extends BaseTransactionObject
    implements VlmProviderObject<RSC>, Comparable<AbsStorageVlmData<RSC>>
{
    // unmodifiable data, once initialized
    protected final AbsVolume<RSC> vlm;
    protected final StorageRscData<RSC> rscData;
    protected final DeviceProviderKind providerKind;

    // persisted, serialized
    protected final TransactionSimpleObject<VlmProviderObject<?>, StorPool> storPool;

    // not persisted, serialized
    // TODO: introduce flags instead of exists, failed, sizeStates, states
    protected final TransactionList<AbsStorageVlmData<RSC>, ? extends State> states;
    protected final TransactionSimpleObject<AbsStorageVlmData<RSC>, Boolean> exists;
    protected final TransactionSimpleObject<AbsStorageVlmData<RSC>, Boolean> failed;
    protected final TransactionSimpleObject<AbsStorageVlmData<RSC>, Long> allocatedSize;
    protected final TransactionSimpleObject<AbsStorageVlmData<RSC>, Long> usableSize;
    protected final TransactionSimpleObject<AbsStorageVlmData<RSC>, String> devicePath;
    protected final TransactionSimpleObject<AbsStorageVlmData<RSC>, Size> sizeState;

    // not persisted, not serialized, stlt only
    protected transient String identifier;
    protected transient long expectedSize;
    protected transient long originalSize;
    protected transient boolean active;

    public AbsStorageVlmData(
        AbsVolume<RSC> vlmRef,
        StorageRscData<RSC> rscDataRef,
        StorPool storPoolRef,
        StorageLayerDatabaseDriver dbDriverRef,
        DeviceProviderKind providerKindRef,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProvider
    )
    {
        super(transMgrProvider);
        vlm = Objects.requireNonNull(vlmRef);
        rscData = Objects.requireNonNull(rscDataRef);
        providerKind = providerKindRef;

        exists = transObjFactory.createTransactionSimpleObject(this, false, null);
        failed = transObjFactory.createTransactionSimpleObject(this, false, null);
        allocatedSize = transObjFactory.createTransactionSimpleObject(this, UNINITIALIZED_SIZE, null);
        usableSize = transObjFactory.createTransactionSimpleObject(this, UNINITIALIZED_SIZE, null);
        devicePath = transObjFactory.createTransactionSimpleObject(this, null, null);
        sizeState = transObjFactory.createTransactionSimpleObject(this, null, null);

        states = transObjFactory.createTransactionList(this, new ArrayList<>(), null);

        storPool = transObjFactory.createTransactionSimpleObject(
            this,
            storPoolRef,
            dbDriverRef.getStorPoolDriver()
        );

        active = true; // by default

        transObjs = new ArrayList<>(
            // this way subclasses can still extend the list
            Arrays.asList(
                vlm,
                rscData,
                exists,
                failed,
                allocatedSize,
                usableSize,
                devicePath,
                sizeState
            )
        );
    }

    @Override
    public Size getSizeState()
    {
        return sizeState.get();
    }

    public void setSizeState(Size sizeStateRef) throws DatabaseException
    {
        sizeState.set(sizeStateRef);
    }

    @Override
    public DeviceLayerKind getLayerKind()
    {
        return DeviceLayerKind.STORAGE;
    }

    @Override
    public DeviceProviderKind getProviderKind()
    {
        return providerKind;
    }

    @Override
    public boolean exists()
    {
        return exists.get();
    }

    public void setExists(boolean existsRef) throws DatabaseException
    {
        exists.set(existsRef);
    }

    @Override
    public boolean hasFailed()
    {
        return failed.get();
    }

    public void setFailed(boolean failedRef) throws DatabaseException
    {
        failed.set(failedRef);
    }

    @Override
    public long getOriginalSize()
    {
        return originalSize;
    }

    @Override
    public void setOriginalSize(long originalSizeRef)
    {
        originalSize = originalSizeRef;
    }

    @Override
    public long getAllocatedSize()
    {
        return allocatedSize.get();
    }

    @Override
    public void setAllocatedSize(long allocatedSizeRef) throws DatabaseException
    {
        allocatedSize.set(allocatedSizeRef);
    }

    @Override
    public long getUsableSize()
    {
        return usableSize.get();
    }

    @Override
    public void setUsableSize(long usableSizeRef) throws DatabaseException
    {
        usableSize.set(usableSizeRef);
    }

    @Override
    public String getDevicePath()
    {
        return devicePath.get();
    }

    public void setDevicePath(String devicePathRef) throws DatabaseException
    {
        devicePath.set(devicePathRef);
    }

    @Override
    public List<? extends State> getStates()
    {
        return states;
    }

    @Override
    public AbsVolume<RSC> getVolume()
    {
        return vlm;
    }

    @Override
    public StorPool getStorPool()
    {
        return storPool.get();
    }

    @Override
    public void setStorPool(AccessContext accCtx, StorPool storPoolRef) throws DatabaseException, AccessDeniedException
    {
        StorPool oldStorPool = storPool.get();
        if (oldStorPool != null)
        {
            if (vlm instanceof Volume)
            {
                oldStorPool.removeVolume(accCtx, (VlmProviderObject<Resource>) this);
            }
            else
            {
                oldStorPool.removeSnapshotVolume(accCtx, (VlmProviderObject<Snapshot>) this);
            }
        }
        storPool.set(storPoolRef);

        if (vlm instanceof Volume)
        {
            storPoolRef.putVolume(accCtx, (VlmProviderObject<Resource>) this);
        }
        else
        {
            storPoolRef.putSnapshotVolume(accCtx, (VlmProviderObject<Snapshot>) this);
        }
    }

    @Override
    public AbsRscLayerObject<RSC> getRscLayerObject()
    {
        return rscData;
    }

    @Override
    public String getIdentifier()
    {
        return identifier;
    }

    public void setIdentifier(String identifierRef)
    {
        identifier = identifierRef;
    }

    public void setExepectedSize(long size)
    {
        expectedSize = size;
    }

    public long getExpectedSize()
    {
        return expectedSize;
    }

    @Override
    public void setActive(boolean activeRef)
    {
        active = activeRef;
    }

    @Override
    public boolean isActive(AccessContext ignored)
    {
        return active;
    }

    @Override
    public int compareTo(AbsStorageVlmData<RSC> other)
    {
        int compareTo = 0;
        AbsVolume<RSC> otherVolume = other.vlm;
        if (vlm instanceof Volume && otherVolume instanceof Volume)
        {
            compareTo = ((Volume) vlm).compareTo((Volume) otherVolume);
        }
        else
        if (vlm instanceof SnapshotVolume && otherVolume instanceof SnapshotVolume)
        {
            compareTo = ((SnapshotVolume) vlm).compareTo((SnapshotVolume) otherVolume);
        }
        else
        {
            throw new ImplementationError(
                "Unknown (other volume) AbsVolume class: " + otherVolume.getClass() +
                    " (local volume: " + vlm.getClass() + ")"
            );
        }
        return compareTo;
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + " [" + vlm.toString() +
            ", RscNameSuffix: " + rscData.getResourceNameSuffix() +
            "]";
    }
}
