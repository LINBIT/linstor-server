package com.linbit.linstor.storage.data.adapter.cache;

import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.pojo.CacheRscPojo;
import com.linbit.linstor.api.pojo.CacheRscPojo.CacheVlmPojo;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.LayerCacheRscDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerCacheVlmDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.AbsRscData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.RscDfnLayerObject;
import com.linbit.linstor.storage.interfaces.layers.cache.CacheRscObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Provider;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CacheRscData<RSC extends AbsResource<RSC>>
    extends AbsRscData<RSC, CacheVlmData<RSC>>
    implements CacheRscObject<RSC>
{
    private final LayerCacheRscDatabaseDriver cacheRscDbDriver;
    private final LayerCacheVlmDatabaseDriver cacheVlmDbDriver;

    public CacheRscData(
        int rscLayerIdRef,
        RSC rscRef,
        AbsRscLayerObject<RSC> parentRef,
        Set<AbsRscLayerObject<RSC>> childrenRef,
        String rscNameSuffixRef,
        LayerCacheRscDatabaseDriver cacheRscDbDriverRef,
        LayerCacheVlmDatabaseDriver cacheVlmDbDriverRef,
        Map<VolumeNumber, CacheVlmData<RSC>> vlmProviderObjectsRef,
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
            cacheRscDbDriverRef.getIdDriver(),
            vlmProviderObjectsRef,
            transObjFactoryRef,
            transMgrProviderRef
        );
        cacheRscDbDriver = cacheRscDbDriverRef;
        cacheVlmDbDriver = cacheVlmDbDriverRef;
    }

    @Override
    public RscDfnLayerObject getRscDfnLayerObject()
    {
        return null;
    }

    @Override
    public RscLayerDataApi asPojo(AccessContext accCtxRef) throws AccessDeniedException
    {
        List<CacheVlmPojo> vlmPojos = new ArrayList<>();
        for (CacheVlmData<RSC> vlmData : vlmMap.values())
        {
            vlmPojos.add(vlmData.asPojo(accCtxRef));
        }
        return new CacheRscPojo(
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
        return DeviceLayerKind.CACHE;
    }

    @Override
    protected void deleteVlmFromDatabase(CacheVlmData<RSC> vlmRef) throws DatabaseException
    {
        cacheVlmDbDriver.delete(vlmRef);
    }

    @Override
    protected void deleteRscFromDatabase() throws DatabaseException
    {
        cacheRscDbDriver.delete(this);
    }

}
