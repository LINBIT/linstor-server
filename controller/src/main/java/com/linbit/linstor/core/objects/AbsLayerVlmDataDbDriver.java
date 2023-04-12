package com.linbit.linstor.core.objects;

import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AbsLayerRscDataDbDriver.ParentObjects;
import com.linbit.linstor.core.objects.AbsLayerRscDataDbDriver.RscDataLoadOutput;
import com.linbit.linstor.core.objects.AbsLayerRscDataDbDriver.SuffixedResourceName;
import com.linbit.linstor.core.objects.AbsLayerVlmDataDbDriver.VlmParentObjects;
import com.linbit.linstor.dbdrivers.AbsDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseTable;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtectionDatabaseDriver;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.utils.Pair;

import java.util.Map;

public abstract class AbsLayerVlmDataDbDriver<
    VLM_DFN_DATA extends VlmDfnLayerObject,
    RSC_DATA extends AbsRscLayerObject<?>,
    VLM_DATA extends VlmProviderObject<?>>
    extends AbsDatabaseDriver<VLM_DATA, Void, VlmParentObjects<VLM_DFN_DATA, RSC_DATA, VLM_DATA>>
{
    public static class VlmParentObjects<
        VLM_DFN_DATA_INNER extends VlmDfnLayerObject,
        RSC_DATA_INNER extends AbsRscLayerObject<?>,
        VLM_DATA_INNER extends VlmProviderObject<?>>
    {
        final Map<Pair<SuffixedResourceName, VolumeNumber>, VLM_DFN_DATA_INNER> vlmDfnDataMap;
        final Map<Integer, RscDataLoadOutput<RSC_DATA_INNER, VLM_DATA_INNER>> rscDataMap;
        final Map<Pair<NodeName, StorPoolName>, Pair<StorPool, StorPool.InitMaps>> storPoolWithInitMap;

        private VlmParentObjects(
            Map<Pair<SuffixedResourceName, VolumeNumber>, VLM_DFN_DATA_INNER> vlmDfnDataMapRef,
            Map<Integer, RscDataLoadOutput<RSC_DATA_INNER, VLM_DATA_INNER>> rscDataMapRef,
            Map<Pair<NodeName, StorPoolName>, Pair<StorPool, StorPool.InitMaps>> storPoolWithInitMapRef
        )
        {
            vlmDfnDataMap = vlmDfnDataMapRef;
            rscDataMap = rscDataMapRef;
            storPoolWithInitMap = storPoolWithInitMapRef;
        }

        protected RSC_DATA_INNER getRscData(int lri)
        {
            return rscDataMap.get(lri).rscData;
        }
    }

    AbsLayerVlmDataDbDriver(
        ErrorReporter errorReporterRef,
        DatabaseTable tableRef,
        DbEngine dbEngineRef,
        ObjectProtectionDatabaseDriver objProtDriverRef
    )
    {
        super(errorReporterRef, tableRef, dbEngineRef, objProtDriverRef);
    }

    @Override
    protected String getId(VLM_DATA vlmDataRef) throws AccessDeniedException
    {
        return "(" + vlmDataRef.getLayerKind().name() +
            ", LayerRscId=" + vlmDataRef.getRscLayerObject().getRscLayerId() +
            ", VlmNr: " + vlmDataRef.getVlmNr().value + ")";
    }

    public void loadAll(
        Map<Pair<SuffixedResourceName, VolumeNumber>, VLM_DFN_DATA> allVlmDfnDataRef,
        Map<Integer, RscDataLoadOutput<RSC_DATA, VLM_DATA>> rscDataByLayerIdRef,
        ParentObjects parentObjectsRef
    )
        throws DatabaseException
    {
        Map<VLM_DATA, Void> vlmDataMap = loadAll(
            new VlmParentObjects<>(
                allVlmDfnDataRef,
                rscDataByLayerIdRef,
                parentObjectsRef.storPoolWithInitMap
            )
        );

        for (VLM_DATA vlmData : vlmDataMap.keySet())
        {
            rscDataByLayerIdRef.get(vlmData.getRscLayerObject().getRscLayerId()).vlmDataMap
                .put(vlmData.getVlmNr(), vlmData);
        }
    }
}
