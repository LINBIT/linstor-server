package com.linbit.linstor.core.objects;

import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AbsLayerRscDataDbDriver.ParentObjects;
import com.linbit.linstor.core.objects.AbsLayerRscDataDbDriver.SuffixedResourceName;
import com.linbit.linstor.core.objects.AbsLayerVlmDfnDataDbDriver.VlmDfnParentObjects;
import com.linbit.linstor.dbdrivers.AbsDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseTable;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.interfaces.categories.resource.RscDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmDfnLayerObject;
import com.linbit.utils.Pair;

import java.util.Map;

public abstract class AbsLayerVlmDfnDataDbDriver<
    RSC_DFN_DATA extends RscDfnLayerObject,
    VLM_DFN_DATA extends VlmDfnLayerObject>
    extends AbsDatabaseDriver<VLM_DFN_DATA, Void, VlmDfnParentObjects<RSC_DFN_DATA>>
{
    static class VlmDfnParentObjects<RSC_DFN_DATA_INNER extends RscDfnLayerObject>
    {
        final Map<ResourceName, ResourceDefinition> rscDfnMap;
        final Map<Pair<ResourceName, SnapshotName>, SnapshotDefinition> snapDfnMap;
        final Map<SuffixedResourceName, RSC_DFN_DATA_INNER> rscDfnDataMap;

        VlmDfnParentObjects(
            Map<ResourceName, ResourceDefinition> rscDfnMapRef,
            Map<Pair<ResourceName, SnapshotName>, SnapshotDefinition> snapDfnMapRef,
            Map<SuffixedResourceName, RSC_DFN_DATA_INNER> rscDfnDataMapRef
        )
        {
            rscDfnMap = rscDfnMapRef;
            snapDfnMap = snapDfnMapRef;
            rscDfnDataMap = rscDfnDataMapRef;
        }
    }

    AbsLayerVlmDfnDataDbDriver(
        AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        DatabaseTable tableRef,
        DbEngine dbEngineRef
    )
    {
        super(dbCtxRef, errorReporterRef, tableRef, dbEngineRef);
    }

    @Override
    protected String getId(VLM_DFN_DATA vlmDfnDataRef) throws AccessDeniedException
    {
        return "(" + vlmDfnDataRef.getLayerKind().name() +
            ", ResName=" + vlmDfnDataRef.getResourceName() +
            ", ResNameSuffix=" + vlmDfnDataRef.getRscNameSuffix() +
            ", SnapName=" + vlmDfnDataRef.getSnapshotName() +
            ", VlmNr=" + vlmDfnDataRef.getVolumeNumber().value + ")";
    }

    void cacheAll(
        ParentObjects parentObjectsRef,
        Map<SuffixedResourceName, RSC_DFN_DATA> allRscDfnDataRef,
        Map<Pair<SuffixedResourceName, VolumeNumber>, VLM_DFN_DATA> allVlmDfnDataRef
    )
        throws DatabaseException, AccessDeniedException
    {
        Map<VLM_DFN_DATA, Void> loadedVlmDfnMap;
        loadedVlmDfnMap = loadAll(
            new VlmDfnParentObjects<>(
                parentObjectsRef.rscDfnMap,
                parentObjectsRef.snapDfnMap,
                allRscDfnDataRef
            )
        );

        for (VLM_DFN_DATA vlmDfnData : loadedVlmDfnMap.keySet())
        {
            allVlmDfnDataRef.put(
                new Pair<>(
                    new SuffixedResourceName(
                        vlmDfnData.getResourceName(),
                        vlmDfnData.getSnapshotName(),
                        vlmDfnData.getRscNameSuffix()
                    ),
                    vlmDfnData.getVolumeNumber()
                ),
                vlmDfnData
            );

            if (vlmDfnData.getSnapshotName() == null)
            {
                ResourceDefinition rscDfn = parentObjectsRef.rscDfnMap.get(vlmDfnData.getResourceName());
                VolumeDefinition vlmDfn = rscDfn.getVolumeDfn(dbCtx, vlmDfnData.getVolumeNumber());
                vlmDfn.setLayerData(dbCtx, vlmDfnData);
            }
            else
            {
                SnapshotDefinition snapDfn = parentObjectsRef.snapDfnMap.get(
                    new Pair<>(vlmDfnData.getResourceName(), vlmDfnData.getSnapshotName())
                );
                SnapshotVolumeDefinition snapVlmDfn = snapDfn.getSnapshotVolumeDefinition(
                    dbCtx,
                    vlmDfnData.getVolumeNumber()
                );
                snapVlmDfn.setLayerData(dbCtx, vlmDfnData);
            }
        }
    }

}
