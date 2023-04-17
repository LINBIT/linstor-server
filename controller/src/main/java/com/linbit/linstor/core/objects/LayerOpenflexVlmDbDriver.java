package com.linbit.linstor.core.objects;

import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.LayerOpenflexVolumes;
import com.linbit.linstor.dbdrivers.interfaces.LayerOpenflexVlmDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.LayerResourceIdDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.ObjectProtectionDatabaseDriver;
import com.linbit.linstor.storage.data.adapter.nvme.OpenflexRscData;
import com.linbit.linstor.storage.data.adapter.nvme.OpenflexVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmDfnLayerObject;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgrSQL;
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.function.Function;

@Singleton
public class LayerOpenflexVlmDbDriver
    extends AbsLayerVlmDataDbDriver<VlmDfnLayerObject, OpenflexRscData<?>, OpenflexVlmData<?>>
    implements LayerOpenflexVlmDatabaseDriver
{
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgrSQL> transMgrProvider;

    private final LayerResourceIdDatabaseDriver rscLayerIdDbDriver;

    @Inject
    public LayerOpenflexVlmDbDriver(
        ErrorReporter errorReporterRef,
        DbEngine dbEngineRef,
        ObjectProtectionDatabaseDriver objProtDriverRef,
        LayerResourceIdDatabaseDriver rscLayerIdDbDriverRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgrSQL> transMgrProviderRef
    )
    {
        super(errorReporterRef, GeneratedDatabaseTables.LAYER_OPENFLEX_VOLUMES, dbEngineRef, objProtDriverRef);
        rscLayerIdDbDriver = rscLayerIdDbDriverRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;

        setColumnSetter(LayerOpenflexVolumes.LAYER_RESOURCE_ID, OpenflexVlmData::getRscLayerId);
        setColumnSetter(LayerOpenflexVolumes.VLM_NR, openflexVlmData -> openflexVlmData.getVlmNr().value);
        setColumnSetter(
            LayerOpenflexVolumes.NODE_NAME,
            openflexVlmData -> openflexVlmDataToStorPool(openflexVlmData, this::storPoolToNodeNameStr)
        );
        setColumnSetter(
            LayerOpenflexVolumes.POOL_NAME,
            openflexVlmData -> openflexVlmDataToStorPool(openflexVlmData, this::storPoolToSpNameStr)
        );
    }

    private String openflexVlmDataToStorPool(
        OpenflexVlmData<?> openflexVlmData,
        Function<StorPool, String> spToStrFunc
    )
    {
        return spToStrFunc.apply(openflexVlmData.getStorPool());
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
    public LayerResourceIdDatabaseDriver getIdDriver()
    {
        return rscLayerIdDbDriver;
    }

    @Override
    protected Pair<OpenflexVlmData<?>, Void> load(
        RawParameters rawRef,
        VlmParentObjects<VlmDfnLayerObject, OpenflexRscData<?>, OpenflexVlmData<?>> loadedParentObjectsRef
    )
        throws ValueOutOfRangeException, InvalidNameException
    {
        int lri = rawRef.getParsed(LayerOpenflexVolumes.LAYER_RESOURCE_ID);
        VolumeNumber vlmNr = rawRef.buildParsed(LayerOpenflexVolumes.VLM_NR, VolumeNumber::new);
        NodeName storPoolNodeName = rawRef.build(LayerOpenflexVolumes.NODE_NAME, NodeName::new);
        StorPoolName storPoolName = rawRef.build(LayerOpenflexVolumes.POOL_NAME, StorPoolName::new);

        OpenflexRscData<?> openflexRscData = loadedParentObjectsRef.getRscData(lri);
        AbsResource<?> absResource = openflexRscData.getAbsResource();

        StorPool storPool = loadedParentObjectsRef.storPoolWithInitMap.get(
            new Pair<>(
                storPoolNodeName,
                storPoolName
            )
        ).objA;

        OpenflexVlmData<?> openflexVlmData = genericCreate(
            absResource.getVolume(vlmNr),
            openflexRscData,
            storPool
        );
        return new Pair<>(openflexVlmData, null);
    }

    @SuppressWarnings("unchecked")
    private <RSC extends AbsResource<RSC>> OpenflexVlmData<RSC> genericCreate(
        AbsVolume<?> vlmRef,
        OpenflexRscData<?> openflexRscDataRef,
        StorPool storPoolRef
    )
    {
        return new OpenflexVlmData<>(
            (AbsVolume<RSC>) vlmRef,
            (OpenflexRscData<RSC>) openflexRscDataRef,
            storPoolRef,
            transObjFactory,
            transMgrProvider
        );
    }
}
