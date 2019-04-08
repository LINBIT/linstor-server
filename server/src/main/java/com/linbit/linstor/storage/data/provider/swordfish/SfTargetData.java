package com.linbit.linstor.storage.data.provider.swordfish;

import com.linbit.linstor.Volume;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.api.pojo.StorageRscPojo.SwordfishTargetVlmPojo;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.storage.data.provider.StorageRscData;
import com.linbit.linstor.storage.interfaces.categories.RscLayerObject;
import com.linbit.linstor.storage.interfaces.layers.State;
import com.linbit.linstor.storage.interfaces.layers.storage.SfTargetVlmProviderObject;
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

public class SfTargetData extends BaseTransactionObject implements SfTargetVlmProviderObject
{
    private static final int ZERO_USABLE_SIZE = 0; // target is never usable, only initiator
    private static final String DEV_NULL = "/dev/null"; // target is never usable, only initiator

    // unmodifiable data, once initialized
    private final Volume vlm;
    private final StorageRscData rscData;
    private final SfVlmDfnData vlmDfnData;

    // not persisted, not serialized, stlt only
    // TODO: introduce flags instead of failed, sizeStates, states
    private final TransactionList<SfTargetData, State> states;
    private final TransactionSimpleObject<SfTargetData, Boolean> failed;
    private final TransactionSimpleObject<SfTargetData, Size> sizeState;
    private final TransactionSimpleObject<SfTargetData, Long> allocatedSize;

    public SfTargetData(
        Volume vlmRef,
        StorageRscData rscDataRef,
        SfVlmDfnData vlmDfnDataRef,
        TransactionObjectFactory transObjFactory,
        Provider<TransactionMgr> transMgrProvider
    )
    {
        super(transMgrProvider);
        vlmDfnData = Objects.requireNonNull(vlmDfnDataRef);
        vlm = Objects.requireNonNull(vlmRef);
        rscData = Objects.requireNonNull(rscDataRef);

        states = transObjFactory.createTransactionList(this, new ArrayList<>(), null);
        failed = transObjFactory.createTransactionSimpleObject(this, false, null);
        sizeState = transObjFactory.createTransactionSimpleObject(this, null, null);
        allocatedSize = transObjFactory.createTransactionSimpleObject(this, -1L, null);

        transObjs = Arrays.asList(
            vlm,
            rscData,
            vlmDfnData,
            states,
            failed,
            sizeState,
            allocatedSize
        );
    }

    @Override
    public boolean exists()
    {
        return vlmDfnData.exists();
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
        return DeviceProviderKind.SWORDFISH_TARGET;
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
        return ZERO_USABLE_SIZE;
    }

    @Override
    public String getDevicePath()
    {
        return DEV_NULL;
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
    public String getVlmOdata()
    {
        return vlmDfnData.getVlmOdata();
    }

    @Override
    public RscLayerObject getRscLayerObject()
    {
        return rscData;
    }
    @Override
    public SfVlmDfnData getVlmDfnLayerObject()
    {
        return vlmDfnData;
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
        return new SwordfishTargetVlmPojo(
            vlmDfnData.getApiData(accCtxRef),
            getAllocatedSize(),
            getUsableSize()
        );
    }
}
