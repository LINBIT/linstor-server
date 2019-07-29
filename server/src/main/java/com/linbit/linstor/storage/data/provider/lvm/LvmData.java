package com.linbit.linstor.storage.data.provider.lvm;

import com.linbit.linstor.StorPool;
import com.linbit.linstor.Volume;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.api.pojo.StorageRscPojo.LvmVlmPojo;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.StorageLayerDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.provider.AbsStorageVlmData;
import com.linbit.linstor.storage.data.provider.StorageRscData;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmDfnLayerObject;
import com.linbit.linstor.storage.interfaces.layers.storage.LvmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.annotation.Nullable;
import javax.inject.Provider;
import java.util.ArrayList;

public class LvmData extends AbsStorageVlmData implements LvmProviderObject
{
    // not persisted, not serialized, stlt only
    private transient String volumeGroup;

    public LvmData(
        Volume vlmRef,
        StorageRscData rscDataRef,
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
            DeviceProviderKind.LVM,
            transObjFactory,
            transMgrProvider
        );
    }

    LvmData(
        Volume vlmRef,
        StorageRscData rscDataRef,
        StorPool storPoolRef,
        StorageLayerDatabaseDriver dbDriverRef,
        DeviceProviderKind kindRef,
        TransactionObjectFactory transObjFactory,
        Provider<TransactionMgr> transMgrProvider
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

    @Override
    public @Nullable VlmDfnLayerObject getVlmDfnLayerObject()
    {
        return null; // no special VlmDfnLayerObject for LVM
    }

    @Override
    public void setStorPool(AccessContext accCtxRef, StorPool storPoolRef) throws DatabaseException, AccessDeniedException
    {
        super.setStorPool(accCtxRef, storPoolRef);
        volumeGroup = null; // force LvmProvider to repeat the lookup using the new storage pool
    }

    public String getVolumeGroup()
    {
        return volumeGroup;
    }

    public void setVolumeGroup(String volumeGroupRef)
    {
        volumeGroup = volumeGroupRef;
    }

    @Override
    public VlmLayerDataApi asPojo(AccessContext accCtxRef) throws AccessDeniedException
    {
        return new LvmVlmPojo(
            getVlmNr().value,
            getDevicePath(),
            getAllocatedSize(),
            getUsableSize(),
            new ArrayList<>(getStates()).toString(), // avoid "TransactionList " in the toString()
            storPool.get().getApiData(null, null, accCtxRef, null, null)
        );
    }
}
