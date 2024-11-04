package com.linbit.linstor.core.objects;

import com.linbit.linstor.core.objects.AbsLayerRscDataDbDriver.ParentObjects;
import com.linbit.linstor.core.objects.AbsLayerRscDataDbDriver.SuffixedResourceName;
import com.linbit.linstor.dbdrivers.AbsDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseTable;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.RscDfnLayerObject;
import com.linbit.utils.Pair;

import java.util.List;
import java.util.Map;

public abstract class AbsLayerRscDfnDataDbDriver<
    RSC_DFN_DATA extends RscDfnLayerObject,
    RSC_DATA extends AbsRscLayerObject<?>>
    extends AbsDatabaseDriver<RSC_DFN_DATA, List<RSC_DATA>, ParentObjects>
{
    AbsLayerRscDfnDataDbDriver(
        AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        DatabaseTable tableRef,
        DbEngine dbEngineRef
    )
    {
        super(dbCtxRef, errorReporterRef, tableRef, dbEngineRef);
    }

    @Override
    protected String getId(RSC_DFN_DATA rscDfnDataRef) throws AccessDeniedException
    {
        return "(" + rscDfnDataRef.getLayerKind().name() +
            ", ResName=" + rscDfnDataRef.getResourceName() +
            ", ResNameSuffix=" + rscDfnDataRef.getRscNameSuffix() +
            ", SnapName=" + rscDfnDataRef.getSnapshotName() + ")";
    }

    void cacheAll(
        ParentObjects parentObjectsRef,
        Map<RSC_DFN_DATA, List<RSC_DATA>> loadedRscDfnChildRscDataMapRef,
        Map<SuffixedResourceName, RSC_DFN_DATA> allRscDfnDataRef
    )
        throws DatabaseException, AccessDeniedException
    {
        loadedRscDfnChildRscDataMapRef.putAll(loadAll(parentObjectsRef));

        for (RSC_DFN_DATA rscDfnData : loadedRscDfnChildRscDataMapRef.keySet())
        {
            allRscDfnDataRef.put(
                new SuffixedResourceName(
                    rscDfnData.getResourceName(),
                    rscDfnData.getSnapshotName(),
                    rscDfnData.getRscNameSuffix()
                ),
                rscDfnData
            );

            if (rscDfnData.getSnapshotName() == null)
            {
                ResourceDefinition rscDfn = parentObjectsRef.rscDfnMap.get(rscDfnData.getResourceName());
                rscDfn.setLayerData(dbCtx, rscDfnData);
            }
            else
            {
                SnapshotDefinition snapDfn = parentObjectsRef.snapDfnMap.get(
                    new Pair<>(rscDfnData.getResourceName(), rscDfnData.getSnapshotName())
                );
                snapDfn.setLayerData(dbCtx, rscDfnData);
            }
        }
    }
}
