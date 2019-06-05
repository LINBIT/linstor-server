package com.linbit.linstor.storage.data.provider.lvm;

import com.linbit.linstor.Volume;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.api.pojo.StorageRscPojo.LvmVlmPojo;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.storage.data.provider.AbsStorageVlmData;
import com.linbit.linstor.storage.data.provider.StorageRscData;
import com.linbit.linstor.storage.interfaces.categories.VlmDfnLayerObject;
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
        TransactionObjectFactory transObjFactory,
        Provider<TransactionMgr> transMgrProvider
    )
    {
        super(
            vlmRef,
            rscDataRef,
            DeviceProviderKind.LVM,
            transObjFactory,
            transMgrProvider
        );
    }

    LvmData(
        Volume vlmRef,
        StorageRscData rscDataRef,
        DeviceProviderKind kindRef,
        TransactionObjectFactory transObjFactory,
        Provider<TransactionMgr> transMgrProvider
    )
    {
        super(
            vlmRef,
            rscDataRef,
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

    public String getVolumeGroup()
    {
        return volumeGroup;
    }

    public void setVolumeGroup(String volumeGroupRef)
    {
        volumeGroup = volumeGroupRef;
    }

    @Override
    public VlmLayerDataApi asPojo(AccessContext accCtxRef)
    {
        return new LvmVlmPojo(
            getVlmNr().value,
            getDevicePath(),
            getAllocatedSize(),
            getUsableSize(),
            new ArrayList<>(getStates()).toString() // avoid "TransactionList " in the toString()
        );
    }
}
