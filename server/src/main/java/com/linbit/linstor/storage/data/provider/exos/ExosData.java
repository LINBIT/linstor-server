package com.linbit.linstor.storage.data.provider.exos;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.api.pojo.StorageRscPojo.ExosVlmPojo;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.AbsVolume;
import com.linbit.linstor.core.objects.StorPool;
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
import com.linbit.linstor.utils.NameShortenerModule;

import javax.inject.Provider;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

@Deprecated(forRemoval = true)
public class ExosData<RSC extends AbsResource<RSC>>
    extends AbsStorageVlmData<RSC> implements LvmProviderObject<RSC>
{
    private @Nullable String shortName;
    private Set<String> hctlSet = new HashSet<>();

    public ExosData(
        AbsVolume<RSC> vlmRef,
        StorageRscData<RSC> rscDataRef,
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
            DeviceProviderKind.EXOS,
            transObjFactory,
            transMgrProvider
        );
    }

    @Override
    public @Nullable VlmDfnLayerObject getVlmDfnLayerObject()
    {
        return null; // no special VlmDfnLayerObject for Exos
    }

    public void updateShortName(AccessContext accCtx) throws AccessDeniedException
    {
        shortName = vlm.getVolumeDefinition().getProps(accCtx).getProp(
            storPool.get().getSharedStorPoolName().displayValue + NameShortenerModule.EXOS_PROP_KEY,
            NameShortenerModule.EXOS_PROP_NAMESPACE
        );
        identifier = shortName;
    }

    public @Nullable String getShortName()
    {
        return shortName;
    }

    public void setHCTL(Set<String> hctlRef)
    {
        hctlSet = hctlRef;
    }

    public Set<String> getHCTLSet()
    {
        return hctlSet;
    }

    @Override
    public VlmLayerDataApi asPojo(AccessContext accCtxRef) throws AccessDeniedException
    {
        return new ExosVlmPojo(
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
}
