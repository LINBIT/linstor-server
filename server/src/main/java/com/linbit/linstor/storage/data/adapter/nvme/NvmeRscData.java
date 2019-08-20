package com.linbit.linstor.storage.data.adapter.nvme;

import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.pojo.NvmeRscPojo;
import com.linbit.linstor.api.pojo.NvmeRscPojo.NvmeVlmPojo;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.RscFlags;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.NvmeLayerDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.AbsRscData;
import com.linbit.linstor.storage.interfaces.categories.resource.RscDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.RscLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Provider;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class NvmeRscData extends AbsRscData<NvmeVlmData>
{
    private boolean exists = false;
    private boolean failed = false;

    public NvmeRscData(
        int rscLayerIdRef,
        Resource rscRef,
        @Nullable RscLayerObject parentRef,
        Set<RscLayerObject> childrenRef,
        Map<VolumeNumber, NvmeVlmData> vlmLayerObjectsMapRef,
        String rscNameSuffixRef,
        NvmeLayerDatabaseDriver dbDriverRef,
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
            dbDriverRef.getIdDriver(),
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
    public @Nullable
    RscDfnLayerObject getRscDfnLayerObject()
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
    public @Nullable RscLayerObject getParent()
    {
        return parent.get();
    }

    @Override
    public void setParent(@Nonnull RscLayerObject parentObj) throws DatabaseException
    {
        parent.set(parentObj);
    }

    public boolean isDiskless(AccessContext accCtx) throws AccessDeniedException
    {
        return rsc.getStateFlags().isSet(accCtx, RscFlags.DISKLESS);
    }

    @Override
    public void delete() throws DatabaseException
    {
        super.delete();
    }

    @Override
    protected void deleteVlmFromDatabase(NvmeVlmData drbdVlmData) throws DatabaseException
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

    public boolean exists()
    {
        return exists;
    }

    public void setExists(boolean existsRef)
    {
        exists = existsRef;
    }

    @Override
    public RscLayerDataApi asPojo(AccessContext accCtx) throws AccessDeniedException
    {
        List<NvmeVlmPojo> vlmPojos = new ArrayList<>();
        for (NvmeVlmData drbdVlmData : vlmMap.values())
        {
            vlmPojos.add(drbdVlmData.asPojo(accCtx));
        }
        return new NvmeRscPojo(
            rscLayerId,
            getChildrenPojos(accCtx),
            getResourceNameSuffix(),
            vlmPojos
        );
    }
}
