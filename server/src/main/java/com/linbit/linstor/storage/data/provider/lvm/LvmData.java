package com.linbit.linstor.storage.data.provider.lvm;

import com.linbit.linstor.Volume;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.api.pojo.StorageRscPojo.LvmVlmPojo;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.storage.data.provider.StorageRscData;
import com.linbit.linstor.storage.interfaces.categories.RscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.VlmDfnLayerObject;
import com.linbit.linstor.storage.interfaces.layers.State;
import com.linbit.linstor.storage.interfaces.layers.storage.LvmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionList;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;

import javax.annotation.Nullable;
import javax.inject.Provider;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class LvmData extends BaseTransactionObject implements LvmProviderObject
{
    // unmodifiable data, once initialized
    private final Volume vlm;
    private final StorageRscData rscData;

    // not persisted, serialized
    // TODO: introduce flags instead of exists, failed, sizeStates, states
    private final TransactionList<LvmData, ? extends State> states;
    private final TransactionSimpleObject<LvmData, Boolean> exists;
    private final TransactionSimpleObject<LvmData, Boolean> failed;
    private final TransactionSimpleObject<LvmData, Long> allocatedSize;
    private final TransactionSimpleObject<LvmData, Long> usableSize;
    private final TransactionSimpleObject<LvmData, String> devicePath;
    private final TransactionSimpleObject<LvmData, Size> sizeState;

    // not persisted, not serialized, stlt only
    private transient String volumeGroup;
    private transient String identifier;

    public LvmData(
        Volume vlmRef,
        StorageRscData rscDataRef,
        TransactionObjectFactory transObjFactory,
        Provider<TransactionMgr> transMgrProvider
    )
    {
        super(transMgrProvider);
        vlm = Objects.requireNonNull(vlmRef);
        rscData = Objects.requireNonNull(rscDataRef);

        exists = transObjFactory.createTransactionSimpleObject(this, false, null);
        failed = transObjFactory.createTransactionSimpleObject(this, false, null);
        allocatedSize = transObjFactory.createTransactionSimpleObject(this, -1L, null);
        usableSize = transObjFactory.createTransactionSimpleObject(this, -1L, null);
        devicePath = transObjFactory.createTransactionSimpleObject(this, null, null);
        sizeState = transObjFactory.createTransactionSimpleObject(this, null, null);

        states = transObjFactory.createTransactionList(this, new ArrayList<>(), null);

        transObjs = new ArrayList<>(
            // this way LvmThinData could extend it (if it would need to)
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
        return DeviceProviderKind.LVM;
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
    public boolean isFailed()
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

    public String getVolumeGroup()
    {
        return volumeGroup;
    }

    public void setVolumeGroup(String volumeGroupRef)
    {
        volumeGroup = volumeGroupRef;
    }

    @Override
    public Volume getVolume()
    {
        return vlm;
    }

    @Override
    public @Nullable VlmDfnLayerObject getVlmDfnLayerObject()
    {
        return null;
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

    @Override
    public VlmLayerDataApi asPojo(AccessContext accCtxRef)
    {
        return new LvmVlmPojo(
            getVlmNr().value,
            getDevicePath(),
            getAllocatedSize(),
            getUsableSize(),
            new ArrayList<>(getStates()).toString() // avoid "TransactionList " in the toString()
        );
    }
}
