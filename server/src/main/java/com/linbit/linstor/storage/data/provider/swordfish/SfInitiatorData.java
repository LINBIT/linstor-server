package com.linbit.linstor.storage.data.provider.swordfish;

import com.linbit.linstor.Volume;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.api.pojo.StorageRscPojo.SwordfishInitiatorVlmPojo;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.storage.data.provider.StorageRscData;
import com.linbit.linstor.storage.interfaces.categories.RscLayerObject;
import com.linbit.linstor.storage.interfaces.layers.State;
import com.linbit.linstor.storage.interfaces.layers.storage.SfInitiatorVlmProviderObject;
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

public class SfInitiatorData extends BaseTransactionObject implements SfInitiatorVlmProviderObject
{
    // unmodifiable data, once initialized
    private final StorageRscData rscData;
    private final Volume vlm;
    private final SfVlmDfnData vlmDfnData;

    // not persisted, serialized
    // TODO: introduce flags instead of exists, failed, sizeStates, states
    private final TransactionList<SfInitiatorData, State> states;
    private final TransactionSimpleObject<SfInitiatorData, Boolean> exists;
    private final TransactionSimpleObject<SfInitiatorData, Boolean> failed;
    private final TransactionSimpleObject<SfInitiatorData, Long> allocatedSize;
    private final TransactionSimpleObject<SfInitiatorData, Long> usableSize;
    private final TransactionSimpleObject<SfInitiatorData, String> devicePath;
    private final TransactionSimpleObject<SfInitiatorData, Size> sizeState;

    public SfInitiatorData(
        StorageRscData rscDataRef,
        Volume vlmRef,
        SfVlmDfnData sfVlmDfnDataRef,
        TransactionObjectFactory transObjFactory,
        Provider<TransactionMgr> transMgrProvider
    )
    {
        super(transMgrProvider);
        rscData = Objects.requireNonNull(rscDataRef);
        vlm = Objects.requireNonNull(vlmRef);
        vlmDfnData = Objects.requireNonNull(sfVlmDfnDataRef);

        states = transObjFactory.createTransactionList(this, new ArrayList<>(), null);
        exists = transObjFactory.createTransactionSimpleObject(this, false, null);
        failed = transObjFactory.createTransactionSimpleObject(this, false, null);
        allocatedSize = transObjFactory.createTransactionSimpleObject(this, -1L, null);
        usableSize = transObjFactory.createTransactionSimpleObject(this, -1L, null);
        devicePath = transObjFactory.createTransactionSimpleObject(this, null, null);
        sizeState = transObjFactory.createTransactionSimpleObject(this, null, null);

        transObjs = Arrays.asList(
            rscData,
            vlm,
            vlmDfnData,
            states,
            exists,
            failed,
            allocatedSize,
            usableSize,
            devicePath,
            sizeState
        );
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
    public DeviceLayerKind getLayerKind()
    {
        return DeviceLayerKind.STORAGE;
    }

    @Override
    public DeviceProviderKind getProviderKind()
    {
        return DeviceProviderKind.SWORDFISH_INITIATOR;
    }

    @Override
    public long getAllocatedSize()
    {
        return allocatedSize.get();
    }

    public void setAllocatedSize(long size) throws SQLException
    {
        allocatedSize.set(size);
    }

    @Override
    public long getUsableSize()
    {
        return usableSize.get();
    }

    public void setUsableSize(long size) throws SQLException
    {
        usableSize.set(size);
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
    public Size getSizeState()
    {
        return sizeState.get();
    }

    @Override
    public List<? extends State> getStates()
    {
        return states;
    }

    @Override
    public SfVlmDfnData getVlmDfnLayerObject()
    {
        return vlmDfnData;
    }

    @Override
    public RscLayerObject getRscLayerObject()
    {
        return rscData;
    }

    @Override
    public Volume getVolume()
    {
        return vlm;
    }

    @Override
    public String getIdentifier()
    {
        return vlmDfnData.getVlmOdata();
    }

    @Override
    public VlmLayerDataApi asPojo(AccessContext accCtxRef)
    {
        return new SwordfishInitiatorVlmPojo(
            vlmDfnData.getApiData(accCtxRef),
            getDevicePath(),
            getAllocatedSize(),
            getUsableSize(),
            new ArrayList<>(getStates()).toString() // avoid "TransactionList " in the toString()
        );
    }
}
