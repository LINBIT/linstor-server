package com.linbit.linstor.layer.snapshot;

import com.linbit.ExhaustedPoolException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.RscDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.utils.LayerDataFactory;

import java.util.Iterator;
import java.util.Map;

public abstract class AbsSnapLayerHelper<
    SNAP_LO extends AbsRscLayerObject<Snapshot>,
    SNAPVLM_LO extends VlmProviderObject<Snapshot>,
    SNAP_DFN_LO extends RscDfnLayerObject,
    SNAPVLM_DFN_LO extends VlmDfnLayerObject
>
{
    protected final ErrorReporter errorReporter;
    protected final AccessContext apiCtx;
    protected final LayerDataFactory layerDataFactory;
    protected final DynamicNumberPool layerRscIdPool;
    protected final DeviceLayerKind kind;

    protected AbsSnapLayerHelper(
        ErrorReporter errorReporterRef,
        AccessContext apiCtxRef,
        LayerDataFactory layerDataFactoryRef,
        DynamicNumberPool layerRscIdPoolRef,
        DeviceLayerKind kindRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        layerDataFactory = layerDataFactoryRef;
        layerRscIdPool = layerRscIdPoolRef;
        kind = kindRef;
    }


    public SNAP_LO copySnapData(
        Snapshot snapRef,
        AbsRscLayerObject<Resource> rscDataRef,
        AbsRscLayerObject<Snapshot> parentRef
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException
    {
        String resourceNameSuffix = rscDataRef.getResourceNameSuffix();
        ensureSnapshotDefinitionExists(
            snapRef.getSnapshotDefinition(),
            resourceNameSuffix
        );

        SNAP_LO snapData;
        if (parentRef != null)
        {
            snapData = (SNAP_LO) parentRef.getChildBySuffix(resourceNameSuffix);
        }
        else
        {
            snapData = (SNAP_LO) snapRef.getLayerData(apiCtx);
        }
        if (snapData == null)
        {
            snapData = createSnapData(snapRef, rscDataRef, parentRef);
        }

        Map<VolumeNumber, SNAPVLM_LO> snapVlmMap = (Map<VolumeNumber, SNAPVLM_LO>) snapData.getVlmLayerObjects();

        Iterator<SnapshotVolume> iterateVolumes = snapRef.iterateVolumes();
        while (iterateVolumes.hasNext())
        {
            SnapshotVolume snapVlm = iterateVolumes.next();
            VolumeNumber vlmNr = snapVlm.getVolumeNumber();

            if (snapVlmMap.get(vlmNr) == null)
            {
                ensureVolumeDefinitionExists(
                    snapVlm.getSnapshotVolumeDefinition(),
                    resourceNameSuffix
                );
                SNAPVLM_LO snapVlmData = createSnapVlmLayerData(
                    snapVlm,
                    snapData,
                    rscDataRef.getVlmProviderObject(vlmNr)
                );
                snapVlmMap.put(vlmNr, snapVlmData);
            }
            // nothing will change / be merged
            // however, this method is called when the Snapshot is created (no volumes)
            // but also for each just created snapshotVolume within this Snapshot

        }
        return snapData;
    }

    /**
     * If the current layer needs a special {@link RscDfnLayerObject}, this method will create it if the given
     * {@link SnapshotDefinition} does not already have a layer data with the given layer type and
     * resource name suffix.
     * If the resource definition already has such an object, it depends on the layer whether the
     * content can be merged / updated or not.
     */
    private SNAP_DFN_LO ensureSnapshotDefinitionExists(
        SnapshotDefinition snapDfn,
        String rscNameSuffix
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException
    {
        SNAP_DFN_LO snapDfnData = snapDfn.getLayerData(apiCtx, kind, rscNameSuffix);
        if (snapDfnData == null)
        {
            snapDfnData = createSnapDfnData(snapDfn, rscNameSuffix);
            if (snapDfnData != null)
            {
                snapDfn.setLayerData(apiCtx, snapDfnData);
            }
        }

        return snapDfnData;
    }

    /**
     * If the current layer needs a special {@link VlmDfnLayerObject}, this method will create it if the given
     * {@link SnapshotVolumeDefinition} does not already have a layer data with the given layer type and
     * resource name suffix.
     * If the volume definition already has such an object, it depends on the layer whether the
     * content can be merged / updated or not.
     */
    private SNAPVLM_DFN_LO ensureVolumeDefinitionExists(
        SnapshotVolumeDefinition snapVlmDfn,
        String rscNameSuffix
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException
    {
        SNAPVLM_DFN_LO snapVlmDfnData = snapVlmDfn.getLayerData(apiCtx, kind, rscNameSuffix);
        if (snapVlmDfnData == null)
        {
            snapVlmDfnData = createSnapVlmDfnData(snapVlmDfn, rscNameSuffix);
            if (snapVlmDfnData != null)
            {
                snapVlmDfn.setLayerData(apiCtx, snapVlmDfnData);
            }
        }
        return snapVlmDfnData;
    }

    protected abstract SNAP_DFN_LO createSnapDfnData(
        SnapshotDefinition rscDfn,
        String rscNameSuffix
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException;

    protected abstract SNAPVLM_DFN_LO createSnapVlmDfnData(
        SnapshotVolumeDefinition snapVlmDfnRef,
        String rscNameSuffixRef
    )
        throws DatabaseException, AccessDeniedException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException;

    protected abstract SNAP_LO createSnapData(
        Snapshot snapRef,
        AbsRscLayerObject<Resource> rscDataRef,
        AbsRscLayerObject<Snapshot> parentRef
    )
        throws AccessDeniedException, DatabaseException, ExhaustedPoolException;

    protected abstract SNAPVLM_LO createSnapVlmLayerData(
        SnapshotVolume snapVlmRef,
        SNAP_LO snapDataRef,
        VlmProviderObject<Resource> vlmProviderObjectRef
    )
        throws DatabaseException, AccessDeniedException;
}
