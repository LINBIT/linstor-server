package com.linbit.linstor.layer.resource;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseLoader;
import com.linbit.linstor.layer.LayerPayload;
import com.linbit.linstor.layer.resource.CtrlRscLayerDataFactory.ChildResourceData;
import com.linbit.linstor.layer.resource.CtrlRscLayerDataFactory.LayerResult;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.RscDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.utils.LayerDataFactory;

import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public abstract class AbsRscLayerHelper<
    RSC_LO extends AbsRscLayerObject<Resource>,
    VLM_LO extends VlmProviderObject<Resource>,
    RSC_DFN_LO extends RscDfnLayerObject,
    VLM_DFN_LO extends VlmDfnLayerObject
>
{
    protected static boolean loadingFromDatabase = true;

    protected final ErrorReporter errorReporter;
    protected final AccessContext apiCtx;
    protected final LayerDataFactory layerDataFactory;
    protected final DynamicNumberPool layerRscIdPool;
    protected final Class<RSC_LO> rscClass;
    protected final DeviceLayerKind kind;
    protected final Provider<CtrlRscLayerDataFactory> layerDataHelperProvider;

    protected AbsRscLayerHelper(
        ErrorReporter errorReporterRef,
        AccessContext apiCtxRef,
        LayerDataFactory layerDataFactoryRef,
        DynamicNumberPool layerRscIdPoolRef,
        Class<RSC_LO> rscClassRef,
        DeviceLayerKind kindRef,
        Provider<CtrlRscLayerDataFactory> layerDataHelperProviderRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        layerDataFactory = layerDataFactoryRef;
        layerRscIdPool = layerRscIdPoolRef;
        rscClass = rscClassRef;
        kind = kindRef;
        layerDataHelperProvider = layerDataHelperProviderRef;
    }

    /**
     * Called after {@link DatabaseLoader} has finished loading objects.
     * The boolean set by this method can be used by all Rsc*LayerHelper to distinguish between initial loading
     * and later creation of new objects.
     *
     * This is needed as i.e. the property "StorPoolNameDrbdMeta" could be set on RD / RG level whereas
     * some resources were created before that property was set and therefore should NOT get an external metadata
     */
    public static void databaseLoadingFinished()
    {
        loadingFromDatabase = false;
    }

    /**
     * If the current layer needs a special {@link RscDfnLayerObject}, this method will create it if the given
     * {@link ResourceDefinition} does not already have a layer data with the given layer type and
     * resource name suffix.
     * If the resource definition already has such an object, it depends on the layer whether the
     * content can be merged / updated or not.
     */
    public RSC_DFN_LO ensureResourceDefinitionExists(
        ResourceDefinition rscDfn,
        String rscNameSuffix,
        LayerPayload payload
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException
    {
        RSC_DFN_LO rscDfnData = rscDfn.getLayerData(apiCtx, kind, rscNameSuffix);
        if (rscDfnData == null)
        {
            rscDfnData = createRscDfnData(rscDfn, rscNameSuffix, payload);
            if (rscDfnData != null)
            {
                rscDfn.setLayerData(apiCtx, rscDfnData);
            }
        }
        else
        {
            mergeRscDfnData(rscDfnData, payload);
        }

        return rscDfnData;
    }

    /**
     * If the current layer needs a special {@link VlmDfnLayerObject}, this method will create it if the given
     * {@link VolumeDefinition} does not already have a layer data with the given layer type and
     * resource name suffix.
     * If the volume definition already has such an object, it depends on the layer whether the
     * content can be merged / updated or not.
     */
    public VLM_DFN_LO ensureVolumeDefinitionExists(
        VolumeDefinition vlmDfn,
        String rscNameSuffix,
        LayerPayload payload
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException
    {
        VLM_DFN_LO vlmDfnData = vlmDfn.getLayerData(apiCtx, kind, rscNameSuffix);
        if (vlmDfnData == null)
        {
            vlmDfnData = createVlmDfnData(vlmDfn, rscNameSuffix, payload);
            vlmDfn.setLayerData(apiCtx, vlmDfnData);
        }
        else
        {
            mergeVlmDfnData(vlmDfnData, payload);
        }
        return vlmDfnData;
    }

    /**
     * Creates a {@link AbsRscLayerObject} if needed and wraps it in a LayerResult which additional contains
     * a list of resource name suffixes for the lower layers.
     * The {@link AbsRscLayerObject} is created if:
     * <ul>
     * <li>the given {@link Resource} has no layer data as root data</li>
     * <li>the parent object is not null and has a child that would match the object this method would create</li>
     * </ul>
     * In case the desired object already exists, the object will be updated if possible according to the data taken
     * from the payload.
     * If the object does not exist a new object is created and added to the parent object's list of children,
     * unless the parent object is null of course.
     *
     * @param layerList
     * @param layerStackRef
     * @throws LinStorException
     * @throws InvalidKeyException
     * @throws InvalidNameException
     */
    @SuppressWarnings("unchecked")
    public LayerResult<Resource> ensureRscDataCreated(
        Resource rscRef,
        LayerPayload payloadRef,
        String rscNameSuffixRef,
        AbsRscLayerObject<Resource> parentObjectRef,
        List<DeviceLayerKind> layerList
    )
        throws DatabaseException, ExhaustedPoolException, ValueOutOfRangeException,
        ValueInUseException, LinStorException, InvalidKeyException, InvalidNameException
    {
        RSC_LO rscData = null;
        if (parentObjectRef == null)
        {
            AbsRscLayerObject<?> rootData = rscRef.getLayerData(apiCtx);
            if (rootData != null && !rootData.getClass().equals(rscClass))
            {
                throw new ImplementationError(
                    "Expected null or instance of " + rscClass.getSimpleName() + ", but got instance of " +
                        rootData.getClass().getSimpleName()
                );
            }
            // Suppressing "unchecked cast" warning as we have already performed a class check.
            rscData = (RSC_LO) rootData;
        }
        else
        {
            for (AbsRscLayerObject<Resource> child : parentObjectRef.getChildren())
            {
                if (rscNameSuffixRef.equals(child.getResourceNameSuffix()))
                {
                    // Suppressing "unchecked cast" warning as the layer list cannot be altered once deployed.
                    rscData = (RSC_LO) child;
                    break;
                }
            }
        }

        if (rscData == null)
        {
            rscData = createRscData(rscRef, payloadRef, rscNameSuffixRef, parentObjectRef, layerList);
        }
        else
        {
            mergeRscData(rscData, payloadRef);
        }

        // create, merge or delete vlmData
        Map<VolumeNumber, VLM_LO> vlmLayerObjects = (Map<VolumeNumber, VLM_LO>) rscData.getVlmLayerObjects();
        List<VolumeNumber> existingVlmsDataToBeDeleted = new ArrayList<>(vlmLayerObjects.keySet());

        Iterator<Volume> iterateVolumes = rscRef.iterateVolumes();
        while (iterateVolumes.hasNext())
        {
            Volume vlm = iterateVolumes.next();

            VolumeDefinition vlmDfn = vlm.getVolumeDefinition();

            VolumeNumber vlmNr = vlmDfn.getVolumeNumber();
            VLM_LO vlmData = vlmLayerObjects.get(vlmNr);
            if (vlmData == null)
            {
                /*
                 * first ask the parent if it needs a child object for this volume
                 * the answer could be no if a DRBD layer has one external (-> "" and ".meta" child)
                 * but also an internal volume.
                 * DRBD LAYER rscObj ""
                 * vlm 0 (external)
                 * vlm 1 (internal)
                 * STORAGE_LAYER rscObj "" // data device for drbd
                 * vlm 0
                 * vlm 1
                 * rscObj ".meta" // meta device for drbd
                 * vlm 0 (need meta-device)
                 * - (does not need meta device)
                 */

                boolean needsChild;
                if (parentObjectRef != null)
                {
                    needsChild = layerDataHelperProvider.get()
                        .getLayerHelperByKind(parentObjectRef.getLayerKind())
                        .needsChildVlm(rscData, vlm);
                }
                else
                {
                    needsChild = true;
                }
                if (needsChild)
                {
                    vlmLayerObjects.put(
                        vlmNr,
                        createVlmLayerData(rscData, vlm, payloadRef, layerList)
                    );
                }
            }
            else
            {
                mergeVlmData(vlmData, vlm, payloadRef, layerList);
            }
            existingVlmsDataToBeDeleted.remove(vlmNr);
        }

        for (VolumeNumber vlmNr : existingVlmsDataToBeDeleted)
        {
            rscData.remove(apiCtx, vlmNr);
        }

        return new LayerResult<>(
            rscData,
            getChildRsc(rscData, layerList)
        );
    }

    public RSC_LO restoreFromSnapshot(
        Resource rsc,
        AbsRscLayerObject<Snapshot> fromSnapDataRef,
        AbsRscLayerObject<Resource> rscParentRef
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException
    {
        ensureResourceDefinitionDataCopiedFromSnapshot(
            rsc.getDefinition(),
            fromSnapDataRef
        );

        RSC_LO rscData = restoreRscData(rsc, fromSnapDataRef, rscParentRef);

        Map<VolumeNumber, VLM_LO> vlmMap = (Map<VolumeNumber, VLM_LO>) rscData.getVlmLayerObjects();
        for (VlmProviderObject<Snapshot> snapVlmData : fromSnapDataRef.getVlmLayerObjects().values())
        {
            VolumeNumber vlmNr = snapVlmData.getVlmNr();
            Volume vlm = rsc.getVolume(vlmNr);

            if (vlm != null && vlmMap.get(vlmNr) == null)
            {
                ensureVolumeDefinitonDataCopiedFromSnapshot(
                    vlm.getVolumeDefinition(),
                    snapVlmData
                );

                VLM_LO vlmData = restoreVlmData(
                    vlm,
                    rscData,
                    fromSnapDataRef.getVlmProviderObject(vlmNr)
                );
                vlmMap.put(vlmNr, vlmData);
            }
        }

        return rscData;
    }

    private RSC_DFN_LO ensureResourceDefinitionDataCopiedFromSnapshot(
        ResourceDefinition rscDfn,
        AbsRscLayerObject<Snapshot> fromSnapData
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException
    {
        RSC_DFN_LO rscDfnData = rscDfn.getLayerData(apiCtx, kind, fromSnapData.getResourceNameSuffix());
        if (rscDfnData == null)
        {
            rscDfnData = restoreRscDfnData(rscDfn, fromSnapData);
            if (rscDfnData != null)
            {
                rscDfn.setLayerData(apiCtx, rscDfnData);
            }
        }
        return rscDfnData;
    }

    private VLM_DFN_LO ensureVolumeDefinitonDataCopiedFromSnapshot(
        VolumeDefinition vlmDfn,
        VlmProviderObject<Snapshot> snapVlmData
    ) throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException
    {
        VLM_DFN_LO vlmDfnData = vlmDfn.getLayerData(
            apiCtx,
            kind,
            snapVlmData.getRscLayerObject().getResourceNameSuffix()
        );
        if (vlmDfnData == null)
        {
            vlmDfnData = restoreVlmDfnData(vlmDfn, snapVlmData);
            if (vlmDfnData != null)
            {
                vlmDfn.setLayerData(apiCtx, vlmDfnData);
            }
        }
        return vlmDfnData;
    }

    /**
     * Only returns the current rscData's resource name suffix by default
     *
     * @param rscDataRef
     * @param layerListRef
     * @return
     * @throws InvalidKeyException
     * @throws AccessDeniedException
     */
    @SuppressWarnings("unused") // exceptions needed by implementations
    protected List<ChildResourceData> getChildRsc(RSC_LO rscDataRef, List<DeviceLayerKind> layerListRef)
        throws AccessDeniedException, InvalidKeyException
    {
        return Arrays.asList(new ChildResourceData(""));
    }

    /**
     * By default, this returns null, meaning that the layer above should be asked
     *
     * @param vlmRef
     * @param childRef
     * @return
     * @throws AccessDeniedException
     * @throws InvalidKeyException
     * @throws InvalidNameException
     */
    @SuppressWarnings("unused") // exceptions needed by implementations
    public StorPool getStorPool(Volume vlmRef, AbsRscLayerObject<Resource> rscDataRef)
        throws AccessDeniedException, InvalidKeyException, InvalidNameException
    {
        return null;
    }

    protected abstract boolean isExpectedToProvideDevice(RSC_LO rsc) throws AccessDeniedException;

    protected abstract RSC_LO createRscData(
        Resource rscRef,
        LayerPayload payloadRef,
        String rscNameSuffixRef,
        AbsRscLayerObject<Resource> parentObjectRef,
        List<DeviceLayerKind> layerListRef
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException, ImplementationError, InvalidNameException;

    protected abstract void mergeRscData(
        RSC_LO rscDataRef,
        LayerPayload payload
    )
        throws AccessDeniedException, DatabaseException;


    protected abstract RSC_DFN_LO createRscDfnData(
        ResourceDefinition rscDfn,
        String rscNameSuffix,
        LayerPayload payload
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException;

    protected abstract void mergeRscDfnData(RSC_DFN_LO rscDfn, LayerPayload payload)
        throws DatabaseException, ExhaustedPoolException, ValueOutOfRangeException, ValueInUseException,
        AccessDeniedException;

    protected abstract VLM_DFN_LO createVlmDfnData(
        VolumeDefinition vlmDfnRef,
        String rscNameSuffixRef,
        LayerPayload payloadRef
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException;

    protected abstract void mergeVlmDfnData(
        VLM_DFN_LO vlmDfnDataRef,
        LayerPayload payloadRef
    );

    protected abstract boolean needsChildVlm(AbsRscLayerObject<Resource> childRscDataRef, Volume vlmRef)
        throws AccessDeniedException, InvalidKeyException;

    protected abstract VLM_LO createVlmLayerData(
        RSC_LO rscDataRef,
        Volume vlm,
        LayerPayload payloadRef,
        List<DeviceLayerKind> layerListRef
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException, LinStorException, InvalidKeyException, InvalidNameException;

    protected abstract void mergeVlmData(
        VLM_LO vlmDataRef,
        Volume vlm,
        LayerPayload payloadRef,
        List<DeviceLayerKind> layerListRef
    )
        throws AccessDeniedException, InvalidKeyException, InvalidNameException, DatabaseException,
        ValueOutOfRangeException, ExhaustedPoolException, ValueInUseException, LinStorException;

    protected abstract void resetStoragePools(AbsRscLayerObject<Resource> rscDataRef)
        throws AccessDeniedException, DatabaseException, InvalidKeyException;

    // abstract methods used for restoring data from snapshot

    protected abstract RSC_DFN_LO restoreRscDfnData(
        ResourceDefinition rscDfn,
        AbsRscLayerObject<Snapshot> fromSnapData
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException;

    protected abstract RSC_LO restoreRscData(
        Resource rsc,
        AbsRscLayerObject<Snapshot> fromSnapDataRef,
        AbsRscLayerObject<Resource> rscParentRef
    )
        throws DatabaseException, AccessDeniedException, ExhaustedPoolException;

    protected abstract VLM_DFN_LO restoreVlmDfnData(
        VolumeDefinition vlmDfn,
        VlmProviderObject<Snapshot> fromSnapVlmData
    )
        throws DatabaseException, AccessDeniedException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException;

    protected abstract VLM_LO restoreVlmData(
        Volume vlm,
        RSC_LO rscDataRef,
        VlmProviderObject<Snapshot> vlmProviderObjectRef
    )
        throws DatabaseException, AccessDeniedException;
}
