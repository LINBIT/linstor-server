package com.linbit.linstor.layer.snapshot;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
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

import java.util.HashMap;
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
        @Nullable AbsRscLayerObject<Snapshot> parentRef
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

            VlmProviderObject<Resource> vlmDataToCopy = rscDataRef.getVlmProviderObject(vlmNr);
            if (snapVlmMap.get(vlmNr) == null && vlmDataToCopy != null)
            {
                /*
                 * in a mixed ext-/internal metadata setup (vlm0=external, vlm1=internal)
                 * vlmDataToCopy might be null when copying the ".meta" path (required for vlm0)
                 * but vlm1 simply has no source to copy from.
                 */
                ensureVolumeDefinitionExists(
                    snapVlm.getSnapshotVolumeDefinition(),
                    resourceNameSuffix
                );
                SNAPVLM_LO snapVlmData = createSnapVlmLayerData(
                    snapVlm,
                    snapData,
                    vlmDataToCopy
                );
                snapVlmMap.put(vlmNr, snapVlmData);
            }
            // nothing will change / be merged
            // however, this method is called when the Snapshot is created (no volumes)
            // but also for each just created snapshotVolume within this Snapshot

        }
        return snapData;
    }

    @SuppressWarnings("unchecked")
    public SNAP_LO restoreSnapData(
        Snapshot snapRef,
        RscLayerDataApi rscLayerDataApiRef,
        @Nullable AbsRscLayerObject<Snapshot> parentRef,
        Map<String, String> renameStorPoolMapRef,
        @Nullable ApiCallRc apiCallRc
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException, InvalidNameException
    {
        String rscNameSuffix = rscLayerDataApiRef.getRscNameSuffix();
        if (snapRef.getSnapshotDefinition().getLayerData(apiCtx, kind, rscNameSuffix) == null)
        {
            SNAP_DFN_LO snapDfnData = restoreSnapDfnData(
                snapRef.getSnapshotDefinition(),
                rscLayerDataApiRef,
                renameStorPoolMapRef
            );
            if (snapDfnData != null)
            {
                snapRef.getSnapshotDefinition().setLayerData(apiCtx, snapDfnData);
            }
        }

        SNAP_LO snapData;
        if (parentRef == null)
        {
            AbsRscLayerObject<Snapshot> rootData = snapRef.getLayerData(apiCtx);
            if (rootData == null)
            {
                snapData = restoreSnapDataImpl(snapRef, rscLayerDataApiRef, parentRef, renameStorPoolMapRef);
            }
            else
            {
                if (!rootData.getLayerKind().equals(kind))
                {
                    throw new ImplementationError(
                        "Expected null or instance with " + kind + ", but got instance with " +
                            rootData.getLayerKind()
                    );
                }
                snapData = (SNAP_LO) rootData;
            }
        }
        else
        {
            AbsRscLayerObject<Snapshot> childData = parentRef.getChildBySuffix(rscNameSuffix);
            if (childData == null)
            {
                snapData = restoreSnapDataImpl(snapRef, rscLayerDataApiRef, parentRef, renameStorPoolMapRef);
            }
            else
            {
                if (!childData.getLayerKind().equals(kind))
                {
                    throw new ImplementationError(
                        "Expected null or instance with " + kind + ", but got instance with " +
                            childData.getLayerKind()
                    );
                }
                snapData = (SNAP_LO) childData;
            }
        }

        Map<VolumeNumber, SNAPVLM_LO> snapVlmMap = (Map<VolumeNumber, SNAPVLM_LO>) snapData.getVlmLayerObjects();

        Iterator<SnapshotVolume> iterateVolumes = snapRef.iterateVolumes();
        Map<Integer, VlmLayerDataApi> vlmLayerDataApiMap = new HashMap<>(rscLayerDataApiRef.getVolumeMap());
        while (iterateVolumes.hasNext())
        {
            SnapshotVolume snapVlm = iterateVolumes.next();
            VolumeNumber vlmNr = snapVlm.getVolumeNumber();

            VlmLayerDataApi vlmLayerDataApi = vlmLayerDataApiMap.get(vlmNr.value);
            if (vlmLayerDataApi == null)
            {
                throw new ImplementationError("No ApiData found for " + snapVlm);
            }

            if (snapVlmMap.get(vlmNr) == null)
            {
                SnapshotVolumeDefinition snapVlmDfn = snapVlm.getSnapshotVolumeDefinition();
                if (snapVlmDfn.getLayerData(apiCtx, kind, rscNameSuffix) == null)
                {
                    SNAPVLM_DFN_LO snapVlmDfnData = restoreSnapVlmDfnData(
                        snapVlmDfn,
                        vlmLayerDataApi,
                        renameStorPoolMapRef
                    );
                    if (snapVlmDfnData != null)
                    {
                        snapVlmDfn.setLayerData(apiCtx, snapVlmDfnData);
                    }
                }

                SNAPVLM_LO snapVlmData = restoreSnapVlmLayerData(
                    snapVlm,
                    snapData,
                    vlmLayerDataApi,
                    renameStorPoolMapRef,
                    apiCallRc
                );
                snapVlmMap.put(vlmNr, snapVlmData);
            }
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
    private @Nullable SNAP_DFN_LO ensureSnapshotDefinitionExists(
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
    private @Nullable SNAPVLM_DFN_LO ensureVolumeDefinitionExists(
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

    protected abstract @Nullable SNAP_DFN_LO createSnapDfnData(
        SnapshotDefinition rscDfn,
        String rscNameSuffix
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException;

    protected abstract @Nullable SNAPVLM_DFN_LO createSnapVlmDfnData(
        SnapshotVolumeDefinition snapVlmDfnRef,
        String rscNameSuffixRef
    )
        throws DatabaseException, AccessDeniedException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException;

    protected abstract SNAP_LO createSnapData(
        Snapshot snapRef,
        AbsRscLayerObject<Resource> rscDataRef,
        @Nullable AbsRscLayerObject<Snapshot> parentRef
    )
        throws AccessDeniedException, DatabaseException, ExhaustedPoolException, ValueOutOfRangeException,
        ValueInUseException;

    protected abstract SNAPVLM_LO createSnapVlmLayerData(
        SnapshotVolume snapVlmRef,
        SNAP_LO snapDataRef,
        VlmProviderObject<Resource> vlmProviderObjectRef
    )
        throws DatabaseException, AccessDeniedException;

    protected abstract @Nullable SNAP_DFN_LO restoreSnapDfnData(
        SnapshotDefinition snapshotDefinitionRef,
        RscLayerDataApi rscLayerDataApiRef,
        Map<String, String> renameStorPoolMapRef
    )
        throws DatabaseException, IllegalArgumentException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException;

    protected abstract @Nullable SNAPVLM_DFN_LO restoreSnapVlmDfnData(
        SnapshotVolumeDefinition snapshotVolumeDefinitionRef,
        VlmLayerDataApi vlmLayerDataApiRef,
        Map<String, String> renameStorPoolMapRef
    )
        throws DatabaseException, AccessDeniedException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException;

    protected abstract SNAP_LO restoreSnapDataImpl(
        Snapshot snapRef,
        RscLayerDataApi rscLayerDataApiRef,
        @Nullable AbsRscLayerObject<Snapshot> parentRef,
        Map<String, String> renameStorPoolMapRef
    )
        throws DatabaseException, ExhaustedPoolException, ValueOutOfRangeException, AccessDeniedException,
        ValueInUseException;

    protected abstract SNAPVLM_LO restoreSnapVlmLayerData(
        SnapshotVolume snapVlmRef,
        SNAP_LO snapDataRef,
        VlmLayerDataApi vlmLayerDataApiRef,
        Map<String, String> renameStorPoolMapRef,
        @Nullable ApiCallRc apiCallRc
    )
        throws AccessDeniedException, InvalidNameException, DatabaseException;
}
