package com.linbit.linstor.storage.data.provider.storagespaces;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.api.pojo.StorageRscPojo.StorageSpacesThinVlmPojo;
import com.linbit.linstor.api.pojo.StorageRscPojo.StorageSpacesVlmPojo;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.AbsVolume;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.dbdrivers.interfaces.LayerStorageVlmDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.provider.AbsStorageVlmData;
import com.linbit.linstor.storage.data.provider.StorageRscData;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmDfnLayerObject;
import com.linbit.linstor.storage.interfaces.layers.storage.StorageSpacesProviderObject;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Provider;

import java.util.ArrayList;

public class StorageSpacesData<RSC extends AbsResource<RSC>>
    extends AbsStorageVlmData<RSC> implements StorageSpacesProviderObject<RSC>
{
    private transient @Nullable String storagePoolFriendlyName;

    public StorageSpacesData(
        AbsVolume<RSC> vlmRef,
        StorageRscData<RSC> rscDataRef,
        DeviceProviderKind kindRef,
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
            kindRef,
            transObjFactory,
            transMgrProvider
        );
    }

    public @Nullable String getStoragePoolFriendlyName()
    {
        return storagePoolFriendlyName;
    }

    public void setStoragePoolFriendlyName(String storagePoolRef)
    {
        storagePoolFriendlyName = storagePoolRef;
    }

    @Override
    public @Nullable VlmDfnLayerObject getVlmDfnLayerObject()
    {
        return null; // no special VlmDfnLayerObject for StorageSpaces
    }

    @Override
    public @Nullable VlmLayerDataApi asPojo(AccessContext accCtxRef) throws AccessDeniedException
    {
        if (providerKind.equals(DeviceProviderKind.STORAGE_SPACES))
        {
            return new StorageSpacesVlmPojo(
                getVlmNr().value,
                getDevicePath(),
                getAllocatedSize(),
                getUsableSize(),
                getSnapshotAllocatedSize(),
                getSnapshotUsableSize(),
                new ArrayList<>(getStates()).toString(), // avoid "TransactionList " in the toString()
                storPool.get().getApiData(null, null, accCtxRef, null, null, null, null),
                exists.get()
            );
        }
        if (providerKind.equals(DeviceProviderKind.STORAGE_SPACES_THIN))
        {
            return new StorageSpacesThinVlmPojo(
                getVlmNr().value,
                getDevicePath(),
                getAllocatedSize(),
                getUsableSize(),
                getSnapshotAllocatedSize(),
                getSnapshotUsableSize(),
                new ArrayList<>(getStates()).toString(), // avoid "TransactionList " in the toString()
                storPool.get().getApiData(null, null, accCtxRef, null, null, null, null),
                exists.get()
            );
        }
        return null;
    }
}
