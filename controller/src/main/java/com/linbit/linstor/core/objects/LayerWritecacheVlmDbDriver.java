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
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.LayerWritecacheVolumes;
import com.linbit.linstor.dbdrivers.RawParameters;
import com.linbit.linstor.dbdrivers.interfaces.LayerResourceIdDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerWritecacheVlmDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.adapter.writecache.WritecacheRscData;
import com.linbit.linstor.storage.data.adapter.writecache.WritecacheVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmDfnLayerObject;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgrSQL;
import com.linbit.utils.Pair;
import com.linbit.utils.PairNonNull;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.function.Function;

@Singleton
public class LayerWritecacheVlmDbDriver
    extends AbsLayerVlmDataDbDriver<VlmDfnLayerObject, WritecacheRscData<?>, WritecacheVlmData<?>>
    implements LayerWritecacheVlmDatabaseDriver
{
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgrSQL> transMgrProvider;
    private final LayerResourceIdDatabaseDriver rscLayerIdDriver;

    @Inject
    public LayerWritecacheVlmDbDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        DbEngine dbEngineRef,
        LayerResourceIdDatabaseDriver rscLayerIdDriverRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgrSQL> transMgrProviderRef
    )
    {
        super(
            dbCtxRef,
            errorReporterRef,
            GeneratedDatabaseTables.LAYER_WRITECACHE_VOLUMES,
            dbEngineRef
        );
        rscLayerIdDriver = rscLayerIdDriverRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;

        setColumnSetter(
            LayerWritecacheVolumes.LAYER_RESOURCE_ID,
            vlmData -> vlmData.getRscLayerObject().getRscLayerId()
        );
        setColumnSetter(LayerWritecacheVolumes.VLM_NR, vlmData -> vlmData.getVlmNr().value);
        setColumnSetter(
            LayerWritecacheVolumes.NODE_NAME,
            vlmData -> writecacheVlmDataToCacheStorPool(vlmData, this::storPoolToNodeNameStr)
        );
        setColumnSetter(
            LayerWritecacheVolumes.POOL_NAME,
            vlmData -> writecacheVlmDataToCacheStorPool(vlmData, this::storPoolToSpNameStr)
        );
    }

    private String writecacheVlmDataToCacheStorPool(
        WritecacheVlmData<?> writecacheVlmData,
        Function<StorPool, String> spToStrFunc
    )
    {
        return spToStrFunc.apply(writecacheVlmData.getCacheStorPool());
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
    protected Pair<WritecacheVlmData<?>, Void> load(
        RawParameters rawRef,
        VlmParentObjects<VlmDfnLayerObject, WritecacheRscData<?>, WritecacheVlmData<?>> parentRef
    )
        throws ValueOutOfRangeException, InvalidNameException, DatabaseException, AccessDeniedException
    {
        int lri = rawRef.getParsed(LayerWritecacheVolumes.LAYER_RESOURCE_ID);
        VolumeNumber vlmNr = rawRef.buildParsed(LayerWritecacheVolumes.VLM_NR, VolumeNumber::new);

        WritecacheRscData<?> writecacheRscData = parentRef.getRscData(lri);
        AbsResource<?> absResource = writecacheRscData.getAbsResource();
        AbsVolume<?> absVlm = absResource.getVolume(vlmNr);

        NodeName storPoolNodeName = rawRef.buildParsed(LayerWritecacheVolumes.NODE_NAME, NodeName::new);
        StorPool cacheStorPool = null;
        if (storPoolNodeName != null)
        {
            StorPoolName storPoolName = rawRef.buildParsed(LayerWritecacheVolumes.POOL_NAME, StorPoolName::new);
            cacheStorPool = parentRef.storPoolWithInitMap.get(
                new PairNonNull<>(
                    storPoolNodeName,
                    storPoolName
                )
            ).objA;
        }

        WritecacheVlmData<?> writecacheVlmData = createAbsVlmData(
            absVlm,
            writecacheRscData,
            cacheStorPool
        );

        return new Pair<>(writecacheVlmData, null);
    }

    @SuppressWarnings("unchecked")
    private <RSC extends AbsResource<RSC>> WritecacheVlmData<RSC> createAbsVlmData(
        AbsVolume<?> absVlmRef,
        WritecacheRscData<?> writecacheRscDataRef,
        @Nullable StorPool cacheStorPoolRef
    )
    {
        return new WritecacheVlmData<>(
            (AbsVolume<RSC>) absVlmRef,
            (WritecacheRscData<RSC>) writecacheRscDataRef,
            cacheStorPoolRef,
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
