package com.linbit.linstor.storage.data.adapter.nvme;

import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.pojo.NvmeRscPojo;
import com.linbit.linstor.api.pojo.NvmeRscPojo.NvmeVlmPojo;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.LayerNvmeRscDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.AbsRscData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.RscDfnLayerObject;
import com.linbit.linstor.storage.interfaces.layers.nvme.NvmeRscObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Provider;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class NvmeRscData<RSC extends AbsResource<RSC>>
    extends AbsRscData<RSC, NvmeVlmData<RSC>>
    implements NvmeRscObject<RSC>
{
    private boolean exists = false;
    private boolean failed = false;
    private boolean spdk = false;

    public NvmeRscData(
        int rscLayerIdRef,
        RSC rscRef,
        @Nullable AbsRscLayerObject<RSC> parentRef,
        Set<AbsRscLayerObject<RSC>> childrenRef,
        Map<VolumeNumber, NvmeVlmData<RSC>> vlmLayerObjectsMapRef,
        String rscNameSuffixRef,
        LayerNvmeRscDatabaseDriver nvmeRscDbDriverRef,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProvider
    )
    {
        super(
            rscLayerIdRef,
            rscRef,
            parentRef,
            childrenRef,
            rscNameSuffixRef,
            nvmeRscDbDriverRef.getIdDriver(),
            vlmLayerObjectsMapRef,
            transObjFactory,
            transMgrProvider
        );

        transObjs = new ArrayList<>(0);
    }

    @Override
    public DeviceLayerKind getLayerKind()
    {
        return DeviceLayerKind.NVME;
    }

    @Override
    public @Nullable RscDfnLayerObject getRscDfnLayerObject()
    {
        return null;
    }

    @Override
    public boolean hasFailed()
    {
        return failed;
    }

    public void setFailed(boolean failedRef)
    {
        failed = failedRef;
    }

    @Override
    public @Nullable AbsRscLayerObject<RSC> getParent()
    {
        return parent.get();
    }

    @Override
    public void setParent(@Nonnull AbsRscLayerObject<RSC> parentObj) throws DatabaseException
    {
        parent.set(parentObj);
    }

    public boolean isInitiator(AccessContext accCtx) throws AccessDeniedException
    {
        boolean isDiskless = false;
        if (rsc instanceof Resource)
        {
            isDiskless = ((Resource) rsc).getStateFlags().isSet(accCtx, Resource.Flags.NVME_INITIATOR);
        }
        return isDiskless;
    }

    @Override
    protected void deleteVlmFromDatabase(NvmeVlmData<RSC> drbdVlmData) throws DatabaseException
    {
        // no-op
    }

    @Override
    protected void deleteRscFromDatabase() throws DatabaseException
    {
        // no-op
    }

    /*
     * Temporary data - will not be persisted
     */

    @Override
    public boolean exists()
    {
        return exists;
    }

    public void setExists(boolean existsRef)
    {
        exists = existsRef;
    }

    public boolean isSpdk()
    {
        return spdk;
    }

    public void setSpdk(boolean spdkRef)
    {
        spdk = spdkRef;
    }

    @Override
    public RscLayerDataApi asPojo(AccessContext accCtx) throws AccessDeniedException
    {
        List<NvmeVlmPojo> vlmPojos = new ArrayList<>();
        for (NvmeVlmData<RSC> nvmeVlmData : vlmMap.values())
        {
            vlmPojos.add(nvmeVlmData.asPojo(accCtx));
        }
        return new NvmeRscPojo(
            rscLayerId,
            getChildrenPojos(accCtx),
            getResourceNameSuffix(),
            vlmPojos,
            suspend.get(),
            ignoreReasons.get()
        );
    }
}
