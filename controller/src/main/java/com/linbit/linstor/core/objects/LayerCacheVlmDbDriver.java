package com.linbit.linstor.core.objects;

import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.LayerCacheVolumes;
import com.linbit.linstor.dbdrivers.RawParameters;
import com.linbit.linstor.dbdrivers.interfaces.LayerCacheVlmDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerResourceIdDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.storage.data.adapter.cache.CacheRscData;
import com.linbit.linstor.storage.data.adapter.cache.CacheVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmDfnLayerObject;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgrSQL;
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.function.Function;

@Singleton
public class LayerCacheVlmDbDriver
    extends AbsLayerVlmDataDbDriver<VlmDfnLayerObject, CacheRscData<?>, CacheVlmData<?>>
    implements LayerCacheVlmDatabaseDriver
{
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgrSQL> transMgrProvider;
    private final LayerResourceIdDatabaseDriver rscLayerIdDriver;

    @Inject
    public LayerCacheVlmDbDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        DbEngine dbEngineRef,
        ObjectProtectionFactory objProtFactoryRef,
        LayerResourceIdDatabaseDriver rscLayerIdDriverRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgrSQL> transMgrProviderRef
    )
    {
        super(dbCtxRef, errorReporterRef, GeneratedDatabaseTables.LAYER_CACHE_VOLUMES, dbEngineRef, objProtFactoryRef);
        rscLayerIdDriver = rscLayerIdDriverRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;

        setColumnSetter(
            LayerCacheVolumes.LAYER_RESOURCE_ID,
            vlmData -> vlmData.getRscLayerObject().getRscLayerId()
        );
        setColumnSetter(LayerCacheVolumes.VLM_NR, vlmData -> vlmData.getVlmNr().value);
        setColumnSetter(
            LayerCacheVolumes.NODE_NAME,
            vlmData -> cacheVlmDataToCacheStorPool(vlmData, this::storPoolToNodeNameStr)
        );
        setColumnSetter(
            LayerCacheVolumes.POOL_NAME_CACHE,
            vlmData -> cacheVlmDataToCacheStorPool(vlmData, this::storPoolToSpNameStr)
        );
        setColumnSetter(
            LayerCacheVolumes.POOL_NAME_META,
            vlmData -> cacheVlmDataToMetaStorPool(vlmData, this::storPoolToSpNameStr)
        );
    }

    private String cacheVlmDataToCacheStorPool(
        CacheVlmData<?> cacheVlmData,
        Function<StorPool, String> spToStrFunc
    )
    {
        String ret = null;
        if (cacheVlmData != null)
        {
            ret = spToStrFunc.apply(cacheVlmData.getCacheStorPool());
        }
        return ret;
    }

    private String cacheVlmDataToMetaStorPool(
        CacheVlmData<?> cacheVlmData,
        Function<StorPool, String> spToStrFunc
    )
    {
        String ret = null;
        if (cacheVlmData != null)
        {
            ret = spToStrFunc.apply(cacheVlmData.getMetaStorPool());
        }
        return ret;
    }

    private String storPoolToNodeNameStr(StorPool sp)
    {
        String ret = null;
        if (sp != null)
        {
            ret = sp.getNode().getName().value;
        }
        return ret;
    }

    private String storPoolToSpNameStr(StorPool sp)
    {
        String ret = null;
        if (sp != null)
        {
            ret = sp.getName().value;
        }
        return ret;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Pair<CacheVlmData<?>, Void> load(
        RawParameters rawRef,
        VlmParentObjects<VlmDfnLayerObject, CacheRscData<?>, CacheVlmData<?>> parentRef
    )
        throws ValueOutOfRangeException, InvalidNameException, DatabaseException, AccessDeniedException
    {
        int lri = rawRef.getParsed(LayerCacheVolumes.LAYER_RESOURCE_ID);
        VolumeNumber vlmNr = rawRef.buildParsed(LayerCacheVolumes.VLM_NR, VolumeNumber::new);

        CacheRscData<?> cacheRscData = parentRef.getRscData(lri);
        AbsResource<?> absResource = cacheRscData.getAbsResource();
        AbsVolume<?> absVlm = absResource.getVolume(vlmNr);

        NodeName storPoolNodeName = rawRef.buildParsed(LayerCacheVolumes.NODE_NAME, NodeName::new);
        StorPool cacheStorPool = null;
        StorPool metaStorPool = null;
        if (storPoolNodeName != null)
        {
            cacheStorPool = parentRef.storPoolWithInitMap.get(
                new Pair<>(
                    storPoolNodeName,
                    rawRef.buildParsed(LayerCacheVolumes.POOL_NAME_CACHE, StorPoolName::new)
                )
            ).objA;
            metaStorPool = parentRef.storPoolWithInitMap.get(
                new Pair<>(
                    storPoolNodeName,
                    rawRef.buildParsed(LayerCacheVolumes.POOL_NAME_META, StorPoolName::new)
                )
            ).objA;
        }

        CacheVlmData<?> cacheVlmData = createAbsVlmData(
            absVlm,
            cacheRscData,
            cacheStorPool,
            metaStorPool
        );

        return new Pair<>(cacheVlmData, null);
    }

    @SuppressWarnings("unchecked")
    private <RSC extends AbsResource<RSC>> CacheVlmData<RSC> createAbsVlmData(
        AbsVolume<?> absVlmRef,
        CacheRscData<?> cacheRscDataRef,
        @Nullable StorPool cacheStorPoolRef,
        @Nullable StorPool metaStorPoolRef
    )
    {
        return new CacheVlmData<>(
            (AbsVolume<RSC>) absVlmRef,
            (CacheRscData<RSC>) cacheRscDataRef,
            cacheStorPoolRef,
            metaStorPoolRef,
            transObjFactory,
            transMgrProvider
        );
    }

    @Override
    public LayerResourceIdDatabaseDriver getIdDriver()
    {
        return rscLayerIdDriver;
    }
}
