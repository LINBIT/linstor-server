package com.linbit.linstor.storage.data.provider;

import com.linbit.linstor.StorPool;
import com.linbit.linstor.Volume;
import com.linbit.linstor.dbdrivers.interfaces.StorageLayerDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.interfaces.categories.resource.RscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.interfaces.layers.State;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionList;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;

import javax.inject.Provider;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public abstract class AbsStorageVlmData extends BaseTransactionObject
    implements VlmProviderObject, Comparable<AbsStorageVlmData>
{
    // unmodifiable data, once initialized
    protected final Volume vlm;
    protected final StorageRscData rscData;
    protected final DeviceProviderKind providerKind;

    // persisted, serialized
    protected final TransactionSimpleObject<VlmProviderObject, StorPool> storPool;

    // not persisted, serialized
    // TODO: introduce flags instead of exists, failed, sizeStates, states
    protected final TransactionList<AbsStorageVlmData, ? extends State> states;
    protected final TransactionSimpleObject<AbsStorageVlmData, Boolean> exists;
    protected final TransactionSimpleObject<AbsStorageVlmData, Boolean> failed;
    protected final TransactionSimpleObject<AbsStorageVlmData, Long> allocatedSize;
    protected final TransactionSimpleObject<AbsStorageVlmData, Long> usableSize;
    protected final TransactionSimpleObject<AbsStorageVlmData, String> devicePath;
    protected final TransactionSimpleObject<AbsStorageVlmData, Size> sizeState;

    // not persisted, not serialized, stlt only
    protected transient String identifier;
    protected transient long expectedSize;

    public AbsStorageVlmData(
        Volume vlmRef,
        StorageRscData rscDataRef,
        StorPool storPoolRef,
        StorageLayerDatabaseDriver dbDriverRef,
        DeviceProviderKind providerKindRef,
        TransactionObjectFactory transObjFactory,
        Provider<TransactionMgr> transMgrProvider
    )
    {
        super(transMgrProvider);
        vlm = Objects.requireNonNull(vlmRef);
        rscData = Objects.requireNonNull(rscDataRef);
        providerKind = providerKindRef;

        exists = transObjFactory.createTransactionSimpleObject(this, false, null);
        failed = transObjFactory.createTransactionSimpleObject(this, false, null);
        allocatedSize = transObjFactory.createTransactionSimpleObject(this, -1L, null);
        usableSize = transObjFactory.createTransactionSimpleObject(this, -1L, null);
        devicePath = transObjFactory.createTransactionSimpleObject(this, null, null);
        sizeState = transObjFactory.createTransactionSimpleObject(this, null, null);

        states = transObjFactory.createTransactionList(this, new ArrayList<>(), null);

        storPool = transObjFactory.createTransactionSimpleObject(
            this,
            storPoolRef,
            dbDriverRef.getStorPoolDriver()
        );

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

    public void setSizeState(Size sizeStateRef) throws SQLException
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

    public void setExists(boolean existsRef) throws SQLException
    {
        exists.set(existsRef);
    }

    @Override
    public boolean hasFailed()
    {
        return failed.get();
    }

    public void setFailed(boolean failedRef) throws SQLException
    {
        failed.set(failedRef);
    }

    @Override
    public long getAllocatedSize()
    {
        return allocatedSize.get();
    }

    public void setAllocatedSize(long allocatedSizeRef) throws SQLException
    {
        allocatedSize.set(allocatedSizeRef);
    }

    @Override
    public long getUsableSize()
    {
        return usableSize.get();
    }

    @Override
    public void setUsableSize(long usableSizeRef) throws SQLException
    {
        usableSize.set(usableSizeRef);
    }

    @Override
    public String getDevicePath()
    {
        return devicePath.get();
    }

    public void setDevicePath(String devicePathRef) throws SQLException
    {
        devicePath.set(devicePathRef);
    }

    @Override
    public List<? extends State> getStates()
    {
        return states;
    }

    @Override
    public Volume getVolume()
    {
        return vlm;
    }

    @Override
    public StorPool getStorPool()
    {
        return storPool.get();
    }

    @Override
    public void setStorPool(AccessContext accCtx, StorPool storPoolRef) throws SQLException, AccessDeniedException
    {
        StorPool oldStorPool = storPool.get();
        if (oldStorPool != null)
        {
            oldStorPool.removeVolume(accCtx, this);
        }
        storPool.set(storPoolRef);
        storPoolRef.putVolume(accCtx, this);
    }

    @Override
    public RscLayerObject getRscLayerObject()
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

    public long getExepectedSize()
    {
        return expectedSize;
    }

    @Override
    public int compareTo(AbsStorageVlmData other)
    {
        return vlm.compareTo(other.vlm);
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + " [" + vlm.toString() +
            ", RscNameSuffix: " + rscData.getResourceNameSuffix() +
            "]";
    }
}
