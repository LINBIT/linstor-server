package com.linbit.linstor.layer.resource;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseLoader;
import com.linbit.linstor.layer.LayerIgnoreReason;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

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
    public @Nullable RSC_DFN_LO ensureResourceDefinitionExists(
        ResourceDefinition rscDfn,
        String rscNameSuffix,
        LayerPayload payload
    )
        throws ValueOutOfRangeException, ExhaustedPoolException, ValueInUseException, LinStorException
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
        ValueInUseException, LinStorException
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
    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE")
    public LayerResult<Resource> ensureRscDataCreated(
        Resource rscRef,
        LayerPayload payloadRef,
        String rscNameSuffixRef,
        @Nullable AbsRscLayerObject<Resource> parentObjectRef,
        List<DeviceLayerKind> layerList,
        Set<LayerIgnoreReason> ignoreReasonsRef
    )
        throws DatabaseException, ExhaustedPoolException, ValueOutOfRangeException,
        ValueInUseException, LinStorException, InvalidKeyException, InvalidNameException
    {
        RSC_LO rscData = null;
        if (parentObjectRef == null)
        {
            // rootData is able to be null at this point, but should never be null later on which is why it is not
            // marked as nullable
            AbsRscLayerObject<?> rootData = rscRef.getLayerData(apiCtx);
            if (rootData != null && !rootData.getClass().equals(rscClass))
            {
                throw new ImplementationError(
                    "Expected instance of " + rscClass.getSimpleName() + ", but got instance of " +
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
            rscData = createRscData(
                rscRef,
                payloadRef,
                rscNameSuffixRef,
                parentObjectRef,
                layerList
            );
            rscData.addAllIgnoreReasons(ignoreReasonsRef);
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

    /**
     * Calling this method will call {@link #addIgnoreReasonImpl(AbsRscLayerObject, String)} of the
     * layerHelper class matching the rscDataRef's DeviceLayerKind AND upwards until the parent is null.
     *
     * If a layerHelper has set the new ignoreReason, its children are also iterated (recursively)
     *
     * @return true if recalculation changed something that requires an updateSatellites, false otherwise
     */
    protected boolean addIgnoreReason(
        @Nullable AbsRscLayerObject<Resource> rscDataRef,
        LayerIgnoreReason ignoreReasonRef,
        boolean goUpRef,
        boolean goDownRef,
        boolean skipSelfRef
    )
        throws DatabaseException
    {
        return addIgnoreReason(
            rscDataRef,
            ignoreReasonRef,
            goUpRef,
            goDownRef,
            skipSelfRef ? rscData -> rscData != rscDataRef : ignored -> true
        );
    }

    protected boolean addIgnoreReason(
        @Nullable AbsRscLayerObject<Resource> rscDataRef,
        LayerIgnoreReason ignoreReasonRef,
        boolean goUpRef,
        boolean goDownRef,
        Predicate<AbsRscLayerObject<Resource>> setIgnoreReasonForRscDataPredicateRef
    )
        throws DatabaseException
    {
        HashSet<AbsRscLayerObject<Resource>> visited = new HashSet<>();
        if (rscDataRef != null)
        {
            if (!goUpRef)
            {
                AbsRscLayerObject<Resource> parent = rscDataRef.getParent();
                if (parent != null)
                {
                    visited.add(parent); // this should implicitly prevent going any higher, no need to add
                    // all ancestors here
                }
            }
            if (!goDownRef)
            {
                for (AbsRscLayerObject<Resource> childRscData : rscDataRef.getChildren())
                {
                    if (childRscData != null)
                    {
                        visited.add(childRscData); // this should implicitly prevent going any lower
                    }
                }
            }
        }
        return addIgnoreReasonRec(
            rscDataRef,
            ignoreReasonRef,
            setIgnoreReasonForRscDataPredicateRef,
            visited
        );
    }

    private boolean addIgnoreReasonRec(
        @Nullable AbsRscLayerObject<Resource> rscDataRef,
        LayerIgnoreReason ignoreReasonRef,
        Predicate<AbsRscLayerObject<Resource>> setIgnoreReasonForRscDataPredicateRef,
        HashSet<AbsRscLayerObject<Resource>> visitedRef
    )
        throws DatabaseException
    {
        boolean changed = false;
        if (rscDataRef != null && !visitedRef.contains(rscDataRef))
        {
            visitedRef.add(rscDataRef);

            AbsRscLayerHelper<?, ?, ?, ?> layerHelperByKind = layerDataHelperProvider.get()
                .getLayerHelperByKind(rscDataRef.getLayerKind());
            Set<LayerIgnoreReason> ignoreReasonPreSet = rscDataRef.getIgnoreReasons();

            boolean setIgnoreReasonForCurrent = setIgnoreReasonForRscDataPredicateRef.test(rscDataRef);
            if (setIgnoreReasonForCurrent)
            {
                changed |= layerHelperByKind.addIgnoreReasonImpl(rscDataRef, ignoreReasonRef);
            }

            // if goUp was set to false, our parent (if not null) was added to visited set so we will soon continue here
            changed |= addIgnoreReasonRec(
                rscDataRef.getParent(),
                ignoreReasonRef,
                setIgnoreReasonForRscDataPredicateRef,
                visitedRef
            );

            Set<LayerIgnoreReason> currentIgnoreReason = rscDataRef.getIgnoreReasons();

            // we only need to process our children if the ignore reason has changed.
            // however, since we are able to skip changing the ignore reason for some rscData, we also have to
            // process our children in those cases.
            boolean hasOwnIgnoreReasonChanged = !Objects.equals(ignoreReasonPreSet, currentIgnoreReason);
            if (!setIgnoreReasonForCurrent || hasOwnIgnoreReasonChanged)
            {
                // if goDown was set to false, our children (if any) were added to visited set so we will
                // come back here soon
                for (AbsRscLayerObject<Resource> childRscData : rscDataRef.getChildren())
                {
                    changed |= addIgnoreReasonRec(
                        childRscData,
                        ignoreReasonRef,
                        setIgnoreReasonForRscDataPredicateRef,
                        visitedRef
                    );
                }
            }
        }
        return changed;
    }

    protected boolean addIgnoreReasonImpl(
        AbsRscLayerObject<Resource> rscDataRef,
        LayerIgnoreReason ignoreReasonRef
    )
        throws DatabaseException
    {
        boolean reasonChanged = false;
        if (!rscDataRef.getIgnoreReasons().contains(ignoreReasonRef))
        {
            rscDataRef.addIgnoreReasons(ignoreReasonRef);
            reasonChanged = true;
        }
        return reasonChanged;
    }

    public <RSC extends AbsResource<RSC>> RSC_LO restoreFromAbsRsc(
        Resource rsc,
        AbsRscLayerObject<RSC> fromAbsRscDataRef,
        @Nullable AbsRscLayerObject<Resource> rscParentRef,
        Map<String, String> storpoolRenameMap,
        @Nullable ApiCallRc apiCallRc
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException, LinStorException, InvalidNameException
    {
        ensureResourceDefinitionDataCopiedFromAbsRsc(
            rsc.getResourceDefinition(),
            fromAbsRscDataRef
        );

        // resource might already have root data, as this method is called from resourceFactory as well as from
        // volumeFactory. That means, creating (or restoring) a resource with one volume will call this method 2 times.
        AbsRscLayerObject<Resource> layerData;
        if (rscParentRef == null)
        {
            layerData = rsc.getLayerData(apiCtx);
        }
        else
        {
            layerData = rscParentRef.getChildBySuffix(fromAbsRscDataRef.getResourceNameSuffix());
        }
        RSC_LO rscData;
        if (layerData == null)
        {
            rscData = restoreRscData(rsc, fromAbsRscDataRef, rscParentRef);
        }
        else
        {
            if (!layerData.getLayerKind().equals(kind))
            {
                throw new ImplementationError(
                    "Layer data already exists but has unexpected kind: " + layerData.getLayerKind() + ". Expected: " +
                        kind
                );
            }
            rscData = (RSC_LO) layerData;
        }

        Map<VolumeNumber, VLM_LO> vlmMap = (Map<VolumeNumber, VLM_LO>) rscData.getVlmLayerObjects();
        for (VlmProviderObject<RSC> snapVlmData : fromAbsRscDataRef.getVlmLayerObjects().values())
        {
            VolumeNumber vlmNr = snapVlmData.getVlmNr();
            Volume vlm = rsc.getVolume(vlmNr);

            if (vlm != null && vlmMap.get(vlmNr) == null)
            {
                ensureVolumeDefinitonDataCopiedFromAbsRsc(
                    vlm.getVolumeDefinition(),
                    snapVlmData
                );

                VLM_LO vlmData = restoreVlmData(
                    vlm,
                    rscData,
                    fromAbsRscDataRef.getVlmProviderObject(vlmNr),
                    storpoolRenameMap,
                    apiCallRc
                );
                vlmMap.put(vlmNr, vlmData);
            }
        }

        return rscData;
    }

    private <RSC extends AbsResource<RSC>> @Nullable RSC_DFN_LO ensureResourceDefinitionDataCopiedFromAbsRsc(
        ResourceDefinition rscDfn,
        AbsRscLayerObject<RSC> fromAbsRscData
    )
        throws ValueOutOfRangeException, ExhaustedPoolException, ValueInUseException, LinStorException
    {
        RSC_DFN_LO rscDfnData = rscDfn.getLayerData(apiCtx, kind, fromAbsRscData.getResourceNameSuffix());
        if (rscDfnData == null)
        {
            rscDfnData = restoreRscDfnData(rscDfn, fromAbsRscData);
            if (rscDfnData != null)
            {
                rscDfn.setLayerData(apiCtx, rscDfnData);
            }
            @Nullable List<DeviceLayerKind> layerStack = getLayerStack(fromAbsRscData);
            if (layerStack != null)
            {
                rscDfn.setLayerStack(apiCtx, layerStack);
            }
        }
        return rscDfnData;
    }

    private @Nullable <RSC extends AbsResource<RSC>> List<DeviceLayerKind> getLayerStack(
        AbsRscLayerObject<RSC> fromAbsRscData
    )
        throws AccessDeniedException
    {
        @Nullable List<DeviceLayerKind> layerStack = null;
        RSC absRsc = fromAbsRscData.getAbsResource();
        if (absRsc instanceof Resource)
        {
            layerStack = absRsc.getResourceDefinition().getLayerStack(apiCtx);
        }
        else if (absRsc instanceof Snapshot)
        {
            layerStack = ((Snapshot) absRsc).getSnapshotDefinition().getLayerStack(apiCtx);
        }
        return layerStack;
    }

    private <RSC extends AbsResource<RSC>> VLM_DFN_LO ensureVolumeDefinitonDataCopiedFromAbsRsc(
        VolumeDefinition vlmDfn,
        VlmProviderObject<RSC> absRscVlmData
    ) throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException
    {
        VLM_DFN_LO vlmDfnData = vlmDfn.getLayerData(
            apiCtx,
            kind,
            absRscVlmData.getRscLayerObject().getResourceNameSuffix()
        );
        if (vlmDfnData == null)
        {
            vlmDfnData = restoreVlmDfnData(vlmDfn, absRscVlmData);
            if (vlmDfnData != null)
            {
                vlmDfn.setLayerData(apiCtx, vlmDfnData);
            }
        }
        return vlmDfnData;
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
    public @Nullable StorPool getStorPool(Volume vlmRef, AbsRscLayerObject<Resource> rscDataRef)
        throws AccessDeniedException, InvalidKeyException, InvalidNameException
    {
        return null;
    }

    protected boolean areAllShared(Set<StorPool> storPools) throws AccessDeniedException
    {
        boolean shared = false;
        boolean nonShared = false;
        for (StorPool sp : storPools)
        {
            if (sp.isShared())
            {
                shared = true;
            }
            else
            {
                nonShared = true;
            }
        }
        if (shared && nonShared)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_PROP,
                    "All or none of the DRBD metadata must be in shared storage pools"
                )
            );
        }
        return shared;
    }

    @SuppressWarnings("unused") // exceptions needed by implementations
    protected abstract List<ChildResourceData> getChildRsc(RSC_LO rscDataRef, List<DeviceLayerKind> layerListRef)
        throws AccessDeniedException, InvalidKeyException;

    protected abstract boolean isExpectedToProvideDevice(RSC_LO rsc) throws AccessDeniedException;

    protected abstract RSC_LO createRscData(
        Resource rscRef,
        LayerPayload payloadRef,
        String rscNameSuffixRef,
        AbsRscLayerObject<Resource> parentObjectRef,
        List<DeviceLayerKind> layerListRef
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
            ValueInUseException, ImplementationError, InvalidNameException, LinStorException;

    protected abstract void mergeRscData(
        RSC_LO rscDataRef,
        LayerPayload payload
    )
        throws AccessDeniedException, DatabaseException, InvalidNameException, ImplementationError,
        ExhaustedPoolException, ValueOutOfRangeException;


    protected abstract @Nullable RSC_DFN_LO createRscDfnData(
        ResourceDefinition rscDfn,
        String rscNameSuffix,
        LayerPayload payload
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException, LinStorException;

    protected abstract void mergeRscDfnData(RSC_DFN_LO rscDfn, LayerPayload payload)
        throws DatabaseException, ExhaustedPoolException, ValueOutOfRangeException, ValueInUseException,
        AccessDeniedException;

    protected abstract VLM_DFN_LO createVlmDfnData(
        VolumeDefinition vlmDfnRef,
        String rscNameSuffixRef,
        LayerPayload payloadRef
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException, LinStorException;

    protected abstract void mergeVlmDfnData(
        VLM_DFN_LO vlmDfnDataRef,
        LayerPayload payloadRef
    );

    protected abstract boolean needsChildVlm(AbsRscLayerObject<Resource> childRscDataRef, Volume vlmRef)
        throws AccessDeniedException, InvalidKeyException;

    protected Set<StorPool> getNeededStoragePools(
        Resource rscRef,
        LayerPayload payloadRef,
        List<DeviceLayerKind> layerListRef
    )
        throws AccessDeniedException, InvalidNameException
    {
        HashSet<StorPool> storPools = new HashSet<>();
        Iterator<VolumeDefinition> vlmDfnIt = rscRef.getResourceDefinition().iterateVolumeDfn(apiCtx);
        while (vlmDfnIt.hasNext())
        {
            VolumeDefinition vlmDfn = vlmDfnIt.next();
            Set<StorPool> neededStoragePools = getNeededStoragePools(rscRef, vlmDfn, payloadRef, layerListRef);
            if (neededStoragePools != null)
            {
                storPools.addAll(neededStoragePools);
            }
        }
        return storPools;
    }

    protected abstract Set<StorPool> getNeededStoragePools(
        Resource rscRef,
        VolumeDefinition vlmDfn,
        LayerPayload payloadRef,
        List<DeviceLayerKind> layerListRef
    )
        throws AccessDeniedException, InvalidNameException;

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

    /**
     * The intention of this method is to (re-)calculate volatile properties as ignoreReason.
     * Such properties are not persisted as well as their requirements can change fast (i.e. Resource.Flags.INACTIVE
     * causes all layers above Storage to be ignored since no device should be provided/accessed when the resource is
     * inactive)
     *
     * @return true if recalculation changed something that requires an updateSatellites, false otherwise
     */
    protected abstract boolean recalculateVolatilePropertiesImpl(
        RSC_LO rscDataRef,
        List<DeviceLayerKind> layerListRef,
        LayerPayload payloadRef
    )
        throws AccessDeniedException, DatabaseException;

    // abstract methods used for restoring data from snapshot

    protected abstract <RSC extends AbsResource<RSC>> @Nullable RSC_DFN_LO restoreRscDfnData(
        ResourceDefinition rscDfn,
        AbsRscLayerObject<RSC> fromAbsRscData
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException, LinStorException;

    protected abstract <RSC extends AbsResource<RSC>> RSC_LO restoreRscData(
        Resource rsc,
        AbsRscLayerObject<RSC> fromAbsRscDataRef,
        AbsRscLayerObject<Resource> rscParentRef
    )
        throws DatabaseException, AccessDeniedException, ExhaustedPoolException;

    protected abstract <RSC extends AbsResource<RSC>> VLM_DFN_LO restoreVlmDfnData(
        VolumeDefinition vlmDfn,
        VlmProviderObject<RSC> fromAbsRscVlmData
    )
        throws DatabaseException, AccessDeniedException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException;

    protected abstract <RSC extends AbsResource<RSC>> VLM_LO restoreVlmData(
        Volume vlm,
        RSC_LO rscDataRef,
        VlmProviderObject<RSC> vlmProviderObjectRef,
        Map<String, String> storpoolRenameMap,
        @Nullable ApiCallRc apiCallRc
    )
        throws DatabaseException, AccessDeniedException, LinStorException, InvalidNameException;
}
