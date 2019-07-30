package com.linbit.linstor.storage.data.provider.swordfish;

import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.api.pojo.StorageRscPojo.SwordfishInitiatorVlmPojo;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.dbdrivers.interfaces.StorageLayerDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.provider.AbsStorageVlmData;
import com.linbit.linstor.storage.data.provider.StorageRscData;
import com.linbit.linstor.storage.interfaces.layers.storage.SfInitiatorVlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Objects;

public class SfInitiatorData extends AbsStorageVlmData implements SfInitiatorVlmProviderObject
{
    // unmodifiable data, once initialized
    private final SfVlmDfnData vlmDfnData;

    public SfInitiatorData(
        StorageRscData rscDataRef,
        Volume vlmRef,
        SfVlmDfnData sfVlmDfnDataRef,
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
            DeviceProviderKind.SWORDFISH_INITIATOR,
            transObjFactory,
            transMgrProvider
        );
        vlmDfnData = Objects.requireNonNull(sfVlmDfnDataRef);

        transObjs.add(vlmDfnData);
    }

    @Override
    public SfVlmDfnData getVlmDfnLayerObject()
    {
        return vlmDfnData;
    }

    @Override
    public String getIdentifier()
    {
        return vlmDfnData.getVlmOdata();
    }

    @Override
    public VlmLayerDataApi asPojo(AccessContext accCtxRef) throws AccessDeniedException
    {
        return new SwordfishInitiatorVlmPojo(
            vlmDfnData.getApiData(accCtxRef),
            getDevicePath(),
            getAllocatedSize(),
            getUsableSize(),
            new ArrayList<>(getStates()).toString(), // avoid "TransactionList " in the toString()
            storPool.get().getApiData(null, null, accCtxRef, null, null)
        );
    }
}
