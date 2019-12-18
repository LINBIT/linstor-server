package com.linbit.linstor.storage.data.provider.lvm;

import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.api.pojo.StorageRscPojo.LvmThinVlmPojo;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.AbsVolume;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.StorageLayerDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.provider.StorageRscData;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Provider;

import java.util.ArrayList;

public class LvmThinData<RSC extends AbsResource<RSC>>
    extends LvmData<RSC>
{
    // not persisted, not serialized, stlt only (copied from storpool)
    private transient String thinPool;
    private transient float dataPercent;

    public LvmThinData(
        AbsVolume<RSC> vlm,
        StorageRscData<RSC> rscData,
        StorPool storPoolRef,
        StorageLayerDatabaseDriver dbDriverRef,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProvider
    )
    {
        super(vlm, rscData, storPoolRef, dbDriverRef, DeviceProviderKind.LVM_THIN, transObjFactory, transMgrProvider);
    }

    @Override
    public void setStorPool(AccessContext accCtxRef, StorPool storPoolRef) throws DatabaseException, AccessDeniedException
    {
        super.setStorPool(accCtxRef, storPoolRef);
        thinPool = null; // force LvmThinProvider to repeat the lookup using the new storage pool
    }

    public String getThinPool()
    {
        return thinPool;
    }

    public void setThinPool(String thinPoolRef)
    {
        thinPool = thinPoolRef;
    }

    public void setAllocatedPercent(float dataPercentRef)
    {
        dataPercent = dataPercentRef;
    }

    public float getDataPercent()
    {
        return dataPercent;
    }

    @Override
    public VlmLayerDataApi asPojo(AccessContext accCtxRef) throws AccessDeniedException
    {
        return new LvmThinVlmPojo(
            getVlmNr().value,
            getDevicePath(),
            getAllocatedSize(),
            getUsableSize(),
            new ArrayList<>(getStates()).toString(), // avoid "TransactionList " in the toString()
            storPool.get().getApiData(null, null, accCtxRef, null, null)
        );
    }
}
