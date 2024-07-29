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
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.LayerBcacheVolumes;
import com.linbit.linstor.dbdrivers.RawParameters;
import com.linbit.linstor.dbdrivers.interfaces.LayerBCacheVlmDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerResourceIdDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.storage.data.adapter.bcache.BCacheRscData;
import com.linbit.linstor.storage.data.adapter.bcache.BCacheVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmDfnLayerObject;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgrSQL;
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.UUID;
import java.util.function.Function;

@Singleton
public class LayerBCacheVlmDbDriver
    extends AbsLayerVlmDataDbDriver<VlmDfnLayerObject, BCacheRscData<?>, BCacheVlmData<?>>
    implements LayerBCacheVlmDatabaseDriver
{
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgrSQL> transMgrProvider;
    private final LayerResourceIdDatabaseDriver rscLayerIdDriver;

    private final SingleColumnDatabaseDriver<BCacheVlmData<?>, UUID> deviceUuidDriver;

    @Inject
    public LayerBCacheVlmDbDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        DbEngine dbEngineRef,
        ObjectProtectionFactory objProtFactoryRef,
        LayerResourceIdDatabaseDriver rscLayerIdDriverRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgrSQL> transMgrProviderRef
    )
    {
        super(dbCtxRef, errorReporterRef, GeneratedDatabaseTables.LAYER_BCACHE_VOLUMES, dbEngineRef, objProtFactoryRef);
        rscLayerIdDriver = rscLayerIdDriverRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;

        setColumnSetter(
            LayerBcacheVolumes.LAYER_RESOURCE_ID,
            vlmData -> vlmData.getRscLayerObject().getRscLayerId()
        );
        setColumnSetter(LayerBcacheVolumes.VLM_NR, vlmData -> vlmData.getVlmNr().value);
        setColumnSetter(
            LayerBcacheVolumes.NODE_NAME,
            vlmData -> bcacheVlmDataToBCacheStorPool(vlmData, this::storPoolToNodeNameStr)
        );
        setColumnSetter(
            LayerBcacheVolumes.POOL_NAME,
            vlmData -> bcacheVlmDataToBCacheStorPool(vlmData, this::storPoolToSpNameStr)
        );
        setColumnSetter(
            LayerBcacheVolumes.DEV_UUID,
            vlmData -> vlmData.getDeviceUuid() == null ? null : vlmData.getDeviceUuid().toString()
        );

        deviceUuidDriver = generateSingleColumnDriver(
            LayerBcacheVolumes.DEV_UUID,
            vlmData -> vlmData.getDeviceUuid().toString(),
            UUID::toString
        );
    }

    private String bcacheVlmDataToBCacheStorPool(
        BCacheVlmData<?> cacheVlmData,
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

    @Override
    protected Pair<BCacheVlmData<?>, Void> load(
        RawParameters rawRef,
        VlmParentObjects<VlmDfnLayerObject, BCacheRscData<?>, BCacheVlmData<?>> parentRef
    )
        throws ValueOutOfRangeException, InvalidNameException, DatabaseException, AccessDeniedException
    {
        int lri = rawRef.getParsed(LayerBcacheVolumes.LAYER_RESOURCE_ID);
        VolumeNumber vlmNr = rawRef.buildParsed(LayerBcacheVolumes.VLM_NR, VolumeNumber::new);

        BCacheRscData<?> cacheRscData = parentRef.getRscData(lri);
        AbsResource<?> absResource = cacheRscData.getAbsResource();
        AbsVolume<?> absVlm = absResource.getVolume(vlmNr);

        NodeName storPoolNodeName = rawRef.buildParsed(LayerBcacheVolumes.NODE_NAME, NodeName::new);
        StorPool cacheStorPool = null;
        if (storPoolNodeName != null)
        {
            cacheStorPool = parentRef.storPoolWithInitMap.get(
                new Pair<>(
                    storPoolNodeName,
                    rawRef.buildParsed(LayerBcacheVolumes.POOL_NAME, StorPoolName::new)
                )
            ).objA;
        }

        BCacheVlmData<?> cacheVlmData = createAbsVlmData(
            absVlm,
            cacheRscData,
            cacheStorPool
        );

        return new Pair<>(cacheVlmData, null);
    }

    @SuppressWarnings("unchecked")
    private <RSC extends AbsResource<RSC>> BCacheVlmData<RSC> createAbsVlmData(
        AbsVolume<?> absVlmRef,
        BCacheRscData<?> cacheRscDataRef,
        @Nullable StorPool cacheStorPoolRef
    )
    {
        return new BCacheVlmData<>(
            (AbsVolume<RSC>) absVlmRef,
            (BCacheRscData<RSC>) cacheRscDataRef,
            cacheStorPoolRef,
            this,
            transObjFactory,
            transMgrProvider
        );
    }

    @Override
    public LayerResourceIdDatabaseDriver getIdDriver()
    {
        return rscLayerIdDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<BCacheVlmData<?>, UUID> getDeviceUuidDriver()
    {
        return deviceUuidDriver;
    }
}
