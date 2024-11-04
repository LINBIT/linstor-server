package com.linbit.linstor.core.objects;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.SystemContext;
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
import com.linbit.linstor.dbdrivers.DatabaseTable.Column;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.utils.Pair;
import com.linbit.utils.PairNonNull;

import java.util.Map;
import java.util.Set;

public abstract class AbsLayerVlmDataDbDriver<
    VLM_DFN_DATA extends VlmDfnLayerObject,
    RSC_DATA extends AbsRscLayerObject<?>,
    VLM_DATA extends VlmProviderObject<?>>
    extends AbsDatabaseDriver<VLM_DATA, Void, VlmParentObjects<VLM_DFN_DATA, RSC_DATA, VLM_DATA>>
{
    /**
     * A special column that is used to populate the RawParameter in case the current (this) *VlmDbDriver does not have
     * a table on its own (like NVMe layer which has no tables or data besides the data stored in LAYER_RESOURCE_IDS).
     * The RawParameters will be populated with only one entry, with this NULL_TABLE_LAYER_VLM_NR_COLUMN as key and the
     * int value of the volume number as value.
     */
    static final NullTableLayerVlmNrColumn NULL_TABLE_LAYER_VLM_NR_COLUMN = new NullTableLayerVlmNrColumn();

    public static class VlmParentObjects<
        VLM_DFN_DATA_INNER extends VlmDfnLayerObject,
        RSC_DATA_INNER extends AbsRscLayerObject<?>,
        VLM_DATA_INNER extends VlmProviderObject<?>>
    {
        final Map<Pair<SuffixedResourceName, VolumeNumber>, VLM_DFN_DATA_INNER> vlmDfnDataMap;
        final Map<Integer, RscDataLoadOutput<RSC_DATA_INNER, VLM_DATA_INNER>> rscDataMap;
        final Map<PairNonNull<NodeName, StorPoolName>, PairNonNull<StorPool, StorPool.InitMaps>> storPoolWithInitMap;

        private VlmParentObjects(
            Map<Pair<SuffixedResourceName, VolumeNumber>, VLM_DFN_DATA_INNER> vlmDfnDataMapRef,
            Map<Integer, RscDataLoadOutput<RSC_DATA_INNER, VLM_DATA_INNER>> rscDataMapRef,
            Map<PairNonNull<NodeName, StorPoolName>, PairNonNull<StorPool, StorPool.InitMaps>> storPoolWithInitMapRef
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
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        @Nullable DatabaseTable tableRef,
        DbEngine dbEngineRef
    )
    {
        super(dbCtxRef, errorReporterRef, tableRef, dbEngineRef);
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
        @Nullable ParentObjects parentObjectsRef
    )
        throws DatabaseException
    {
        Set<VLM_DATA> vlmDataSet = super.loadAll(
            new VlmParentObjects<>(
                allVlmDfnDataRef,
                rscDataByLayerIdRef,
                parentObjectsRef.storPoolWithInitMap
            )
        ).keySet();

        for (VLM_DATA vlmData : vlmDataSet)
        {
            rscDataByLayerIdRef.get(vlmData.getRscLayerObject().getRscLayerId()).vlmDataMap
                .put(vlmData.getVlmNr(), vlmData);
        }
    }

    static class NullTableLayerVlmNrColumn implements Column
    {
        protected static final String NULL_TABLE_LAYER_VLM_NR_OBJ = "NullTableLayerVlmNrObj";

        @Override
        public String getName()
        {
            return NULL_TABLE_LAYER_VLM_NR_OBJ;
        }

        @Override
        public int getSqlType()
        {
            return java.sql.Types.OTHER;
        }

        @Override
        public boolean isPk()
        {
            return false;
        }

        @Override
        public boolean isNullable()
        {
            return false;
        }

        @Override
        public @Nullable DatabaseTable getTable()
        {
            return null;
        }
    }
}
