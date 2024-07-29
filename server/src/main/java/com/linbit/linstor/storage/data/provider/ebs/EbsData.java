package com.linbit.linstor.storage.data.provider.ebs;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.api.pojo.StorageRscPojo.EbsVlmPojo;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.AbsVolume;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.LayerStorageVlmDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.provider.AbsStorageVlmData;
import com.linbit.linstor.storage.data.provider.StorageRscData;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmDfnLayerObject;
import com.linbit.linstor.storage.interfaces.layers.storage.LvmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Provider;

import java.util.ArrayList;

public class EbsData<RSC extends AbsResource<RSC>>
    extends AbsStorageVlmData<RSC> implements LvmProviderObject<RSC>
{
    public EbsData(
        AbsVolume<RSC> vlmRef,
        StorageRscData<RSC> rscDataRef,
        DeviceProviderKind providerKindRef,
        StorPool storPoolRef,
        LayerStorageVlmDatabaseDriver dbDriverRef,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProvider
    )
    {
        super(
            vlmRef,
            rscDataRef,
            storPoolRef,
            dbDriverRef,
            providerKindRef,
            transObjFactory,
            transMgrProvider
        );
    }

    @Override
    public @Nullable VlmDfnLayerObject getVlmDfnLayerObject()
    {
        return null; // no special VlmDfnLayerObject for EBS
    }

    @Override
    public VlmLayerDataApi asPojo(AccessContext accCtxRef) throws AccessDeniedException
    {
        return new EbsVlmPojo(
            getVlmNr().value,
            getDevicePath(),
            getAllocatedSize(),
            getUsableSize(),
            getSnapshotAllocatedSize(),
            getSnapshotUsableSize(),
            new ArrayList<>(getStates()).toString(), // avoid "TransactionList " in the toString()
            discGran.get(),
            storPool.get().getApiData(null, null, accCtxRef, null, null, null, null),
            exists.get()
        );
    }

    @Override
    public void setAllocatedSize(long allocatedSizeRef) throws DatabaseException
    {
        super.setAllocatedSize(allocatedSizeRef);
    }

    @Override
    public void setUsableSize(long usableSizeRef) throws DatabaseException
    {
        // TODO Auto-generated method stub
        super.setUsableSize(usableSizeRef);
    }

}
