package com.linbit.linstor.storage.data.provider.lvm;

import com.linbit.linstor.Volume;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.api.pojo.StorageRscPojo.LvmThinVlmPojo;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.storage.data.provider.StorageRscData;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Provider;

import java.util.ArrayList;

public class LvmThinData extends LvmData
{
    // not persisted, not serialized, stlt only (copied from storpool)
    private transient String thinPool;
    private transient float dataPercent;

    public LvmThinData(
        Volume vlm,
        StorageRscData rscData,
        TransactionObjectFactory transObjFactory,
        Provider<TransactionMgr> transMgrProvider
    )
    {
        super(vlm, rscData, transObjFactory, transMgrProvider);
    }

    public String getThinPool()
    {
        return thinPool;
    }

    public void setThinPool(String thinPoolRef)
    {
        thinPool = thinPoolRef;
    }

    @Override
    public DeviceProviderKind getProviderKind()
    {
        return DeviceProviderKind.LVM_THIN;
    }

    @Override
    public VlmLayerDataApi asPojo(AccessContext accCtxRef)
    {
        return new LvmThinVlmPojo(
            getVlmNr().value,
            getDevicePath(),
            getAllocatedSize(),
            getUsableSize(),
            new ArrayList<>(getStates()).toString() // avoid "TransactionList " in the toString()
        );
    }

    public void setAllocatedPercent(float dataPercentRef)
    {
        dataPercent = dataPercentRef;
    }

    public float getDataPercent()
    {
        return dataPercent;
    }
}
