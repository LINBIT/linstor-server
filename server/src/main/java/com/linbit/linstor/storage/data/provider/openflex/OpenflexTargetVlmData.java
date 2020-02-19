package com.linbit.linstor.storage.data.provider.openflex;

import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.api.pojo.StorageRscPojo.OpenflexVlmPojo;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.AbsVolume;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.StorageLayerDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.provider.AbsStorageVlmData;
import com.linbit.linstor.storage.data.provider.StorageRscData;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmDfnLayerObject;
import com.linbit.linstor.storage.interfaces.layers.storage.OpenflexTargetVlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Provider;

import java.util.ArrayList;

public class OpenflexTargetVlmData<RSC extends AbsResource<RSC>>
    extends AbsStorageVlmData<RSC>
    implements OpenflexTargetVlmProviderObject<RSC>
{
    private static final int ZERO_USABLE_SIZE = 0; // target is never usable, only initiator
    private static final String DEV_NULL = "/dev/null"; // target is never usable, only initiator
    // unmodifiable data, once initialized

    public OpenflexTargetVlmData(
        AbsVolume<RSC> vlmRef,
        StorageRscData<RSC> rscDataRef,
        StorPool storPoolRef,
        StorageLayerDatabaseDriver dbDriverRef,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProvider
    )
    {
        super(
            vlmRef,
            rscDataRef,
            storPoolRef,
            dbDriverRef,
            DeviceProviderKind.OPENFLEX_TARGET,
            transObjFactory,
            transMgrProvider
        );
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
    public VlmDfnLayerObject getVlmDfnLayerObject()
    {
        return null; // no special VlmDfnLayerObject for Openflex
    }

    @Override
    public String getDevicePath()
    {
        return DEV_NULL;
    }

    @Override
    public VlmLayerDataApi asPojo(AccessContext accCtxRef) throws AccessDeniedException
    {
        return new OpenflexVlmPojo(
            getVlmNr().value,
            getDevicePath(),
            getAllocatedSize(),
            getUsableSize(),
            new ArrayList<>(getStates()).toString(), // avoid "TransactionList " in the toString()
            storPool.get().getApiData(null, null, accCtxRef, null, null)
        );
    }
}
