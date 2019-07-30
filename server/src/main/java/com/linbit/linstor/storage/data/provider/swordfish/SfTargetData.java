package com.linbit.linstor.storage.data.provider.swordfish;

import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.api.pojo.StorageRscPojo.SwordfishTargetVlmPojo;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.StorageLayerDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.provider.AbsStorageVlmData;
import com.linbit.linstor.storage.data.provider.StorageRscData;
import com.linbit.linstor.storage.interfaces.layers.storage.SfTargetVlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Provider;
import java.util.Objects;

public class SfTargetData extends AbsStorageVlmData implements SfTargetVlmProviderObject
{
    private static final int ZERO_USABLE_SIZE = 0; // target is never usable, only initiator
    private static final String DEV_NULL = "/dev/null"; // target is never usable, only initiator

    // unmodifiable data, once initialized
    private final SfVlmDfnData vlmDfnData;

    public SfTargetData(
        Volume vlmRef,
        StorageRscData rscDataRef,
        SfVlmDfnData vlmDfnDataRef,
        StorPool storPoolRef,
        StorageLayerDatabaseDriver dbDriverRef,
        TransactionObjectFactory transObjFactory,
        Provider<TransactionMgr> transMgrProvider
    )
    {
        super(
            vlmRef,
            rscDataRef,
            storPoolRef,
            dbDriverRef,
            DeviceProviderKind.SWORDFISH_TARGET,
            transObjFactory,
            transMgrProvider
        );
        vlmDfnData = Objects.requireNonNull(vlmDfnDataRef);

        transObjs.add(vlmDfnDataRef);
    }

    @Override
    public boolean exists()
    {
        return vlmDfnData.exists();
    }

    @Override
    public long getUsableSize()
    {
        return ZERO_USABLE_SIZE;
    }

    @Override
    public void setUsableSize(long netSizeRef) throws DatabaseException
    {
        // no-op
        // TODO: sure about this no-op?
    }

    @Override
    public String getDevicePath()
    {
        return DEV_NULL;
    }

    @Override
    public String getVlmOdata()
    {
        return vlmDfnData.getVlmOdata();
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
    public VlmLayerDataApi asPojo(AccessContext accCtxRef) throws AccessDeniedException
    {
        return new SwordfishTargetVlmPojo(
            vlmDfnData.getApiData(accCtxRef),
            getAllocatedSize(),
            getUsableSize(),
            storPool.get().getApiData(null, null, accCtxRef, null, null)
        );
    }
}
