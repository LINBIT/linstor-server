package com.linbit.linstor.storage.data.provider.diskless;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.api.pojo.StorageRscPojo.DisklessVlmPojo;
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
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Provider;

public class DisklessData<RSC extends AbsResource<RSC>>
    extends AbsStorageVlmData<RSC>
{
    public DisklessData(
        AbsVolume<RSC> vlmRef,
        StorageRscData<RSC> rscDataRef,
        long usableSizeRef,
        StorPool storPoolRef,
        LayerStorageVlmDatabaseDriver dbDriverRef,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProvider
    )
        throws DatabaseException
    {
        super(
            vlmRef,
            rscDataRef,
            storPoolRef,
            dbDriverRef,
            DeviceProviderKind.DISKLESS,
            transObjFactory,
            transMgrProvider
        );
        usableSize.set(usableSizeRef);
    }

    @Override
    public @Nullable String getDevicePath()
    {
        return null;
    }

    @Override
    public long getAllocatedSize()
    {
        return 0L;
    }

    @Override
    public Size getSizeState()
    {
        return Size.AS_EXPECTED;
    }

    @Override
    public @Nullable VlmDfnLayerObject getVlmDfnLayerObject()
    {
        return null; // no special VlmDfnLayerObject for DISKLESS
    }

    @Override
    public String getIdentifier()
    {
        return rscData.getSuffixedResourceName() + "/" + getVlmNr().value;
    }

    @Override
    public VlmLayerDataApi asPojo(AccessContext accCtxRef) throws AccessDeniedException
    {
        return new DisklessVlmPojo(
            vlm.getVolumeNumber().value,
            getDevicePath(),
            getAllocatedSize(),
            getUsableSize(),
            null,
            null,
            null,
            discGran.get(),
            storPool.get().getApiData(null, null, accCtxRef, null, null, null, null),
            exists.get()
        );
    }
}
