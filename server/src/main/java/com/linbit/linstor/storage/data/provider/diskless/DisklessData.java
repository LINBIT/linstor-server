package com.linbit.linstor.storage.data.provider.diskless;

import com.linbit.linstor.Volume;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.api.pojo.StorageRscPojo.DisklessVlmPojo;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.storage.data.provider.AbsStorageVlmData;
import com.linbit.linstor.storage.data.provider.StorageRscData;
import com.linbit.linstor.storage.interfaces.categories.VlmDfnLayerObject;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import javax.annotation.Nullable;
import javax.inject.Provider;

import java.sql.SQLException;

public class DisklessData extends AbsStorageVlmData
{
    public DisklessData(
        Volume vlmRef,
        StorageRscData rscDataRef,
        long usableSizeRef,
        TransactionObjectFactory transObjFactory,
        Provider<TransactionMgr> transMgrProvider
    )
        throws SQLException
    {
        super(
            vlmRef,
            rscDataRef,
            DeviceProviderKind.DISKLESS,
            transObjFactory,
            transMgrProvider
        );
        usableSize.set(usableSizeRef);
    }

    @Override
    public String getDevicePath()
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
    public VlmLayerDataApi asPojo(AccessContext accCtxRef)
    {
        return new DisklessVlmPojo(
            vlm.getVolumeDefinition().getVolumeNumber().value,
            getDevicePath(),
            getAllocatedSize(),
            getUsableSize(),
            null
        );
    }
}
