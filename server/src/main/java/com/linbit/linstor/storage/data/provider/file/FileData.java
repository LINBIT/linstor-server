package com.linbit.linstor.storage.data.provider.file;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.api.pojo.StorageRscPojo.FileVlmPojo;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.AbsVolume;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.dbdrivers.interfaces.LayerStorageVlmDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.provider.AbsStorageVlmData;
import com.linbit.linstor.storage.data.provider.StorageRscData;
import com.linbit.linstor.storage.interfaces.categories.FileProviderObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmDfnLayerObject;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Provider;

import java.nio.file.Path;
import java.util.ArrayList;

public class FileData<RSC extends AbsResource<RSC>>
    extends AbsStorageVlmData<RSC> implements FileProviderObject<RSC>
{
    // not persisted, not serialized, stlt only
    private transient @Nullable Path storageDir;

    public FileData(
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

    public @Nullable Path getStorageDirectory()
    {
        return storageDir;
    }

    public void setStorageDirectory(@Nullable Path storageDirectoryRef)
    {
        storageDir = storageDirectoryRef;
    }

    @Override
    public @Nullable VlmDfnLayerObject getVlmDfnLayerObject()
    {
        return null; // no special VlmDfnLayerObject for FileProvider
    }

    @Override
    public VlmLayerDataApi asPojo(AccessContext accCtxRef) throws AccessDeniedException
    {
        return new FileVlmPojo(
            vlm.getVolumeNumber().getValue(),
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
}
