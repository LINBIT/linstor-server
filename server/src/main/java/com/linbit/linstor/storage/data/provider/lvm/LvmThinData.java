package com.linbit.linstor.storage.data.provider.lvm;

import com.linbit.linstor.Volume;
import com.linbit.linstor.api.interfaces.VlmLayerDataPojo;
import com.linbit.linstor.api.pojo.StorageRscPojo.LvmThinVlmPojo;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.storage.data.provider.StorageRscData;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Provider;

public class LvmThinData extends LvmData
{
    // not persisted, not serialized, stlt only (copied from storpool)
    private transient String thinPool;

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
    public VlmLayerDataPojo asPojo(AccessContext accCtxRef)
    {
        return new LvmThinVlmPojo(
            getVlmNr().value,
            getDevicePath(),
            getAllocatedSize(),
            getUsableSize()
        );
    }
}
