package com.linbit.linstor.storage.data.adapter.bcache;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.pojo.BCacheRscPojo;
import com.linbit.linstor.api.pojo.BCacheRscPojo.BCacheVlmPojo;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.LayerBCacheRscDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerBCacheVlmDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.AbsRscData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.RscDfnLayerObject;
import com.linbit.linstor.storage.interfaces.layers.writecache.WritecacheRscObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Provider;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class BCacheRscData<RSC extends AbsResource<RSC>>
    extends AbsRscData<RSC, BCacheVlmData<RSC>>
    implements WritecacheRscObject<RSC>
{
    private final LayerBCacheRscDatabaseDriver bcacheRscDbDriver;
    private final LayerBCacheVlmDatabaseDriver bcacheVlmDbDriver;

    public BCacheRscData(
        int rscLayerIdRef,
        RSC rscRef,
        @Nullable AbsRscLayerObject<RSC> parentRef,
        Set<AbsRscLayerObject<RSC>> childrenRef,
        String rscNameSuffixRef,
        LayerBCacheRscDatabaseDriver bcacheRscDbDriverRef,
        LayerBCacheVlmDatabaseDriver bcacheVlmDbDriverRef,
        Map<VolumeNumber, BCacheVlmData<RSC>> vlmProviderObjectsRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<? extends TransactionMgr> transMgrProviderRef
    )
    {
        super(
            rscLayerIdRef,
            rscRef,
            parentRef,
            childrenRef,
            rscNameSuffixRef,
            bcacheRscDbDriverRef.getIdDriver(),
            vlmProviderObjectsRef,
            transObjFactoryRef,
            transMgrProviderRef
        );
        bcacheRscDbDriver = bcacheRscDbDriverRef;
        bcacheVlmDbDriver = bcacheVlmDbDriverRef;
    }

    @Override
    public @Nullable RscDfnLayerObject getRscDfnLayerObject()
    {
        return null;
    }

    @Override
    public RscLayerDataApi asPojo(AccessContext accCtxRef) throws AccessDeniedException
    {
        List<BCacheVlmPojo> vlmPojos = new ArrayList<>();
        for (BCacheVlmData<RSC> vlmData : vlmMap.values())
        {
            vlmPojos.add(vlmData.asPojo(accCtxRef));
        }
        return new BCacheRscPojo(
            rscLayerId,
            getChildrenPojos(accCtxRef),
            rscSuffix,
            vlmPojos,
            suspend.get(),
            ignoreReasons.get()
        );
    }

    @Override
    public DeviceLayerKind getLayerKind()
    {
        return DeviceLayerKind.BCACHE;
    }

    @Override
    protected void deleteVlmFromDatabase(BCacheVlmData<RSC> vlmRef) throws DatabaseException
    {
        bcacheVlmDbDriver.delete(vlmRef);
    }

    @Override
    protected void deleteRscFromDatabase() throws DatabaseException
    {
        bcacheRscDbDriver.delete(this);
    }
}
