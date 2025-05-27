package com.linbit.linstor.layer.resource;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.CtrlStorPoolResolveHelper;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.LayerIgnoreReason;
import com.linbit.linstor.layer.LayerPayload;
import com.linbit.linstor.layer.LayerPayload.DrbdRscDfnPayload;
import com.linbit.linstor.layer.LayerPayload.StorageVlmPayload;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscDfnData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.utils.LayerUtils;
import com.linbit.linstor.utils.layer.LayerRscUtils;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Singleton
public class CtrlRscLayerDataFactory
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final CtrlStorPoolResolveHelper storPoolResolveHelper;
    private final RscDrbdLayerHelper drbdLayerHelper;
    private final RscLuksLayerHelper luksLayerHelper;
    private final RscStorageLayerHelper storageLayerHelper;
    private final RscNvmeLayerHelper nvmeLayerHelper;
    private final RscWritecacheLayerHelper writecacheLayerHelper;
    private final RscCacheLayerHelper cacheLayerHelper;
    private final RscBCacheLayerHelper bcacheLayerHelper;

    @Inject
    public CtrlRscLayerDataFactory(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        CtrlStorPoolResolveHelper storPoolResolveHelperRef,
        RscDrbdLayerHelper drbdLayerHelperRef,
        RscLuksLayerHelper luksLayerHelperRef,
        RscStorageLayerHelper storageLayerHelperRef,
        RscNvmeLayerHelper nvmeLayerHelperRef,
        RscWritecacheLayerHelper writecacheLayerHelperRef,
        RscCacheLayerHelper cacheLayerHelperRef,
        RscBCacheLayerHelper bcacheLayerHelperRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        storPoolResolveHelper = storPoolResolveHelperRef;
        drbdLayerHelper = drbdLayerHelperRef;
        luksLayerHelper = luksLayerHelperRef;
        storageLayerHelper = storageLayerHelperRef;
        nvmeLayerHelper = nvmeLayerHelperRef;
        writecacheLayerHelper = writecacheLayerHelperRef;
        cacheLayerHelper = cacheLayerHelperRef;
        bcacheLayerHelper = bcacheLayerHelperRef;
    }

    public List<DeviceLayerKind> getLayerStack(Resource rscRef)
    {
        List<DeviceLayerKind> layerStack;
        try
        {
            layerStack = LayerRscUtils.getLayerStack(rscRef, apiCtx);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return layerStack;
    }

    /**
     * Creates the linstor default stack, which is {@link DeviceLayerKind#DRBD} on an optional
     * {@link DeviceLayerKind#LUKS} on a {@link DeviceLayerKind#STORAGE} layer.
     * A LUKS layer is created if at least one {@link VolumeDefinition}
     * has the {@link VolumeDefinition.Flags#ENCRYPTED} flag set.
     *
     * @param accCtxRef
     * @return
     */
    public List<DeviceLayerKind> createDefaultStack(AccessContext accCtxRef, Resource rscRef)
    {
        List<DeviceLayerKind> layerStack;
        try
        {
            // drbd + (luks) + storage
            if (needsLuksLayer(accCtxRef, rscRef))
            {
                layerStack = Arrays.asList(
                    DeviceLayerKind.DRBD,
                    DeviceLayerKind.LUKS,
                    DeviceLayerKind.STORAGE
                );
            }
            else
            {
                layerStack = Arrays.asList(
                    DeviceLayerKind.DRBD,
                    DeviceLayerKind.STORAGE
                );
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return layerStack;
    }

    public void ensureStackDataExists(
        Resource rscRef,
        @Nullable List<DeviceLayerKind> layerListRef,
        LayerPayload payload
    )
    {
        try
        {
            List<DeviceLayerKind> layerList = new ArrayList<>();
            if (layerListRef == null || layerListRef.isEmpty())
            {
                layerList = LayerUtils.getLayerStack(rscRef, apiCtx);
            }
            else
            {
                layerList.addAll(layerListRef);
            }

            if (layerList.isEmpty())
            {
                throw new ImplementationError("Cannot create resource with empty layer list");
            }

            AbsRscLayerObject<Resource> rootObj = null;

            rootObj = ensureDataRec(rscRef, payload, layerList, new ChildResourceData(""), null);

            rscRef.setLayerData(apiCtx, rootObj);
            recalculateVolatileRscData(rscRef);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (ExhaustedPoolException exc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_POOL_EXHAUSTED_RSC_LAYER_ID,
                    "No TCP/IP port number could be allocated for the resource"
                )
                    .setCause("The pool of free TCP/IP port numbers is exhausted")
                    .setCorrection(
                        "- Adjust the TcpPortAutoRange controller configuration value to extend the range\n" +
                            "  of TCP/IP port numbers used for automatic allocation\n" +
                            "- Delete unused resource definitions that occupy TCP/IP port numbers from the range\n" +
                            "  used for automatic allocation\n"
                    ),
                exc
            );
        }
        catch (DatabaseException exc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_SQL,
                    "A database exception occurred while creating layer data"
                ),
                exc
            );
        }
        catch (Exception exc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_UNKNOWN_ERROR,
                    "An exception occurred while creating layer data"
                ),
                exc
            );
        }
    }

    private @Nullable AbsRscLayerObject<Resource> ensureDataRec(
        Resource rscRef,
        LayerPayload payloadRef,
        List<DeviceLayerKind> layerStackRef,
        ChildResourceData currentData,
        @Nullable AbsRscLayerObject<Resource> parentRscObj
    )
        throws InvalidKeyException, DatabaseException, ExhaustedPoolException, ValueOutOfRangeException,
        ValueInUseException, LinStorException, InvalidNameException
    {
        DeviceLayerKind currentKind = layerStackRef.get(0);
        List<DeviceLayerKind> childList = layerStackRef.subList(1, layerStackRef.size());

        AbsRscLayerObject<Resource> ret;

        if (currentData.skipUntilList.contains(currentKind))
        {
            AbsRscLayerHelper<?, ?, ?, ?> layerHelper = getLayerHelperByKind(currentKind);
            LayerResult<Resource> result = layerHelper.ensureRscDataCreated(
                rscRef,
                payloadRef,
                currentData.rscNameSuffix,
                parentRscObj,
                childList,
                currentData.ignoreReasons
            );
            ret = result.rscObj;

            for (ChildResourceData childRsc : result.childRsc)
            {
                AbsRscLayerObject<Resource> childRscLayerData = ensureDataRec(
                    rscRef,
                    payloadRef,
                    childList,
                    childRsc,
                    result.rscObj
                );
                result.rscObj.getChildren().add(childRscLayerData);
            }
        }
        else
        {
            // pass through to next layer
            ret = ensureDataRec(rscRef, payloadRef, childList, currentData, parentRscObj);
        }
        return ret;
    }

    public void resetStoragePools(Resource rscRef)
    {
        try
        {
            LinkedList<AbsRscLayerObject<Resource>> rscDataToProcess = new LinkedList<>();
            rscDataToProcess.add(rscRef.getLayerData(apiCtx));

            while (!rscDataToProcess.isEmpty())
            {
                AbsRscLayerObject<Resource> rscData = rscDataToProcess.removeFirst();
                getLayerHelperByKind(rscData.getLayerKind()).resetStoragePools(rscData);

                rscDataToProcess.addAll(rscData.getChildren());
            }

            ensureStackDataExists(rscRef, null, new LayerPayload());
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (DatabaseException exc)
        {
            errorReporter.reportError(exc);
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_SQL,
                    "A database exception occurred while creating layer data"
                ),
                exc
            );
        }
        catch (Exception exc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_UNKNOWN_ERROR,
                    "An exception occurred while creating layer data"
                ),
                exc
            );
        }
    }

    public void ensureRequiredRscDfnLayerDataExits(
        ResourceDefinition rscDfn,
        String rscNameSuffix,
        LayerPayload payload
    )
        throws DatabaseException, LinStorException, ValueOutOfRangeException
    {
        List<DeviceLayerKind> layerStack;
        try
        {
            layerStack = rscDfn.getLayerStack(apiCtx);
            DrbdRscDfnPayload drbdRscDfn = payload.drbdRscDfn;
            if (layerStack.contains(DeviceLayerKind.DRBD) ||
                drbdRscDfn.tcpPort != null ||
                drbdRscDfn.transportType != null ||
                drbdRscDfn.sharedSecret != null ||
                drbdRscDfn.peerSlotsNewResource != null
            )
            {
                drbdLayerHelper.ensureResourceDefinitionExists(
                    rscDfn,
                    rscNameSuffix,
                    payload
                );
            }
            else
            {
                DrbdRscDfnData<Resource> drbdRscDfnData = rscDfn.getLayerData(
                    apiCtx,
                    DeviceLayerKind.DRBD,
                    rscNameSuffix
                );
                if (drbdRscDfnData != null)
                {
                    rscDfn.removeLayerData(
                        apiCtx,
                        DeviceLayerKind.DRBD,
                        rscNameSuffix
                    );
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    public void ensureVlmDfnLayerDataExits(
        VolumeDefinition vlmDfn,
        String rscNameSuffix,
        LayerPayload payload
    )
        throws DatabaseException, ValueOutOfRangeException,
            ExhaustedPoolException, ValueInUseException, LinStorException
    {
        try
        {
            List<DeviceLayerKind> layerStack = vlmDfn.getResourceDefinition().getLayerStack(apiCtx);
            if (layerStack.contains(DeviceLayerKind.DRBD) || payload.drbdVlmDfn.minorNr != null)
            {
                drbdLayerHelper.ensureVolumeDefinitionExists(
                    vlmDfn,
                    rscNameSuffix,
                    payload
                );
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    public StorPool getStorPool(
        Volume vlmRef,
        AbsRscLayerObject<Resource> rscDataRef,
        LayerPayload payloadRef
    )
        throws AccessDeniedException, InvalidKeyException, InvalidNameException
    {
        StorPool storPool = null;

        StorageVlmPayload storageVlmPayload = payloadRef.getStorageVlmPayload(
            rscDataRef.getResourceNameSuffix(),
            vlmRef.getVolumeDefinition().getVolumeNumber().value
        );
        if (storageVlmPayload != null)
        {
            storPool = storageVlmPayload.storPool;
        }

        AbsRscLayerObject<Resource> child = rscDataRef;
        AbsRscLayerObject<Resource> parent = rscDataRef.getParent();
        while (parent != null && storPool == null)
        {
            AbsRscLayerHelper<?, ?, ?, ?> layerHelper = getLayerHelperByKind(parent.getLayerKind());

            storPool = layerHelper.getStorPool(vlmRef, child);

            child = parent;
            parent = parent.getParent();
        }
        if (storPool == null)
        {
            ApiCallRcImpl dummyApiCallRc = new ApiCallRcImpl();
            Resource rsc = vlmRef.getAbsResource();
            VolumeDefinition vlmDfn = vlmRef.getVolumeDefinition();

            storPool = storPoolResolveHelper.resolveStorPool(
                rsc,
                vlmDfn,
                isDiskless(rsc) && !isDiskAddRequested(rsc),
                isDiskRemoving(rsc)
            ).extractApiCallRc(dummyApiCallRc);
        }
        return storPool;
    }

    /*
     * This method should be replaced by most likely a rework of the LayerData-creation:
     * It would be easier if we would create a (or multiple) builder(s) where all data are collected
     * for every layer, but are still modifiable. That way we could i.e. only input storPoolNames and resolve
     * them later. The selection of the storPoolName can this way be much easier split from the resolving / using
     * the StorPool.
     */
    HashSet<StorPool> getAllNeededStorPools(
        Resource rsc,
        LayerPayload payload,
        List<DeviceLayerKind> layerList
    )
        throws AccessDeniedException, InvalidNameException
    {
        HashSet<StorPool> storPools = new HashSet<>();
        for (DeviceLayerKind kind : layerList)
        {
            AbsRscLayerHelper<?, ?, ?, ?> layerHelper = getLayerHelperByKind(kind);
            Set<StorPool> neededStoragePools = layerHelper.getNeededStoragePools(rsc, payload, layerList);
            storPools.addAll(neededStoragePools);
        }
        return storPools;
    }

    public <RSC extends AbsResource<RSC>> void copyLayerData(
        AbsRscLayerObject<RSC> fromAbsRsc,
        Resource toResource,
        Map<String, String> storpoolRenameMap,
        @Nullable ApiCallRc apiCallRc
    )
    {
        try
        {
            AbsRscLayerObject<Resource> rscData = copyRec(toResource, fromAbsRsc, null, storpoolRenameMap, apiCallRc);
            toResource.setLayerData(apiCtx, rscData);
            clearIgnoreReasonsRec(rscData);
            recalculateVolatileRscData(toResource);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (DatabaseException exc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_SQL,
                    "A database exception occurred while creating layer data"
                ),
                exc
            );
        }
        catch (ExhaustedPoolException exc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_POOL_EXHAUSTED_RSC_LAYER_ID,
                    "No TCP/IP port number could be allocated for the resource"
                )
                    .setCause("The pool of free TCP/IP port numbers is exhausted")
                    .setCorrection(
                        "- Adjust the TcpPortAutoRange controller configuration value to extend the range\n" +
                            "  of TCP/IP port numbers used for automatic allocation\n" +
                            "- Delete unused resource definitions that occupy TCP/IP port numbers from the range\n" +
                            "  used for automatic allocation\n"
                    ),
                exc
            );
        }
        catch (LinStorException lsExc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_UNKNOWN_ERROR,
                    "Volume definition creation failed due to an unidentified error code, see text message " +
                    "of nested exception"
                ),
                lsExc
            );
        }
        catch (Exception exc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_UNKNOWN_ERROR,
                    "An exception occurred while creating layer data"
                ),
                exc
            );
        }
    }

    private <RSC extends AbsResource<RSC>> AbsRscLayerObject<Resource> copyRec(
        Resource rsc,
        AbsRscLayerObject<RSC> fromRscData,
        @Nullable AbsRscLayerObject<Resource> rscParentRef,
        Map<String, String> storpoolRenameMap,
        @Nullable ApiCallRc apiCallRc
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException, LinStorException, InvalidNameException
    {
        AbsRscLayerHelper<?, ?, ?, ?> layerHelper = getLayerHelperByKind(fromRscData.getLayerKind());

        AbsRscLayerObject<Resource> rscData = layerHelper.restoreFromAbsRsc(
            rsc,
            fromRscData,
            rscParentRef,
            storpoolRenameMap,
            apiCallRc
        );

        for (AbsRscLayerObject<RSC> snapChild : fromRscData.getChildren())
        {
            rscData.getChildren().add(copyRec(rsc, snapChild, rscData, storpoolRenameMap, apiCallRc));
        }
        return rscData;
    }

    private void clearIgnoreReasonsRec(AbsRscLayerObject<Resource> rscDataRef) throws DatabaseException
    {
        rscDataRef.clearIgnoreReasons();
        for (AbsRscLayerObject<Resource> child : rscDataRef.getChildren())
        {
            clearIgnoreReasonsRec(child);
        }
    }

    public boolean recalculateVolatileRscData(Resource rscRef) throws AccessDeniedException, DatabaseException
    {
        AbsRscLayerObject<Resource> rscData = rscRef.getLayerData(apiCtx);
        List<DeviceLayerKind> layerStack = getLayerStack(rscRef);
        clearIgnoreReasonsRec(rscData);
        return recalculateVolatileRscDataRec(rscData, layerStack, new LayerPayload());
    }

    @SuppressWarnings("unchecked")
    private <RSC_LO extends AbsRscLayerObject<Resource>> boolean recalculateVolatileRscDataRec(
        AbsRscLayerObject<Resource> rscDataRef,
        List<DeviceLayerKind> layerStackRef,
        LayerPayload layerPayloadRef
    )
        throws AccessDeniedException, DatabaseException
    {
        AbsRscLayerHelper<RSC_LO, ?, ?, ?> layerHelper = getLayerHelperByKind(rscDataRef.getLayerKind());
        layerHelper.recalculateVolatilePropertiesImpl((RSC_LO) rscDataRef, layerStackRef, layerPayloadRef);

        boolean changed = false;
        for (AbsRscLayerObject<Resource> child : rscDataRef.getChildren())
        {
            changed |= recalculateVolatileRscDataRec(child, layerStackRef, layerPayloadRef);
        }
        return changed;
    }

    protected <RSC_LO extends AbsRscLayerObject<Resource>> AbsRscLayerHelper<RSC_LO, ?, ?, ?> getLayerHelperByKind(
        DeviceLayerKind kind
    )
    {
        AbsRscLayerHelper<RSC_LO, ?, ?, ?> layerHelper;
        switch (kind)
        {
            case DRBD:
                layerHelper = (AbsRscLayerHelper<RSC_LO, ?, ?, ?>) drbdLayerHelper;
                break;
            case LUKS:
                layerHelper = (AbsRscLayerHelper<RSC_LO, ?, ?, ?>) luksLayerHelper;
                break;
            case NVME:
                layerHelper = (AbsRscLayerHelper<RSC_LO, ?, ?, ?>) nvmeLayerHelper;
                break;
            case WRITECACHE:
                layerHelper = (AbsRscLayerHelper<RSC_LO, ?, ?, ?>) writecacheLayerHelper;
                break;
            case CACHE:
                layerHelper = (AbsRscLayerHelper<RSC_LO, ?, ?, ?>) cacheLayerHelper;
                break;
            case BCACHE:
                layerHelper = (AbsRscLayerHelper<RSC_LO, ?, ?, ?>) bcacheLayerHelper;
                break;
            case STORAGE:
                layerHelper = (AbsRscLayerHelper<RSC_LO, ?, ?, ?>) storageLayerHelper;
                break;
            default:
                throw new ImplementationError("Unknown device layer kind '" + kind + "'");
        }
        return layerHelper;
    }

    protected boolean needsLuksLayer(AccessContext accCtxRef, Resource rscRef)
        throws AccessDeniedException
    {
        boolean needsLuksLayer = false;
        Iterator<VolumeDefinition> iterateVolumeDefinitions = rscRef.getResourceDefinition().iterateVolumeDfn(apiCtx);
        while (iterateVolumeDefinitions.hasNext())
        {
            VolumeDefinition vlmDfn = iterateVolumeDefinitions.next();

            if (vlmDfn.getFlags().isSet(accCtxRef, VolumeDefinition.Flags.ENCRYPTED))
            {
                needsLuksLayer = true;
                break;
            }
        }
        return needsLuksLayer;
    }

    boolean isDiskAddRequested(Resource rsc)
    {
        boolean isDiskAddRequested;
        try
        {
            isDiskAddRequested = rsc.getStateFlags().isSet(apiCtx, Resource.Flags.DISK_ADD_REQUESTED);
        }
        catch (AccessDeniedException implError)
        {
            throw new ImplementationError(implError);
        }
        return isDiskAddRequested;
    }

    boolean isDiskless(Resource rsc)
    {
        boolean isDiskless;
        try
        {
            StateFlags<Flags> stateFlags = rsc.getStateFlags();
            isDiskless = stateFlags.isSomeSet(
                apiCtx,
                Resource.Flags.DRBD_DISKLESS,
                Resource.Flags.NVME_INITIATOR,
                Resource.Flags.EBS_INITIATOR
            );
        }
        catch (AccessDeniedException implError)
        {
            throw new ImplementationError(implError);
        }
        return isDiskless;
    }

    boolean isDiskRemoving(Resource rsc)
    {
        boolean isDiskless;
        try
        {
            isDiskless = rsc.getStateFlags().isSet(apiCtx, Resource.Flags.DISK_REMOVING);
        }
        catch (AccessDeniedException implError)
        {
            throw new ImplementationError(implError);
        }
        return isDiskless;
    }

    public static class LayerResult<RSC extends AbsResource<RSC>>
    {
        private @Nullable AbsRscLayerObject<RSC> rscObj;
        private List<ChildResourceData> childRsc = new ArrayList<>();

        LayerResult(@Nullable AbsRscLayerObject<RSC> rscObjRef)
        {
            rscObj = rscObjRef;
            childRsc.add(new ChildResourceData(""));
        }

        LayerResult(@Nullable AbsRscLayerObject<RSC> rscObjref, List<ChildResourceData> childRscListRef)
        {
            rscObj = rscObjref;
            childRsc.addAll(childRscListRef);
        }

        @Override
        public String toString()
        {
            return "LayerResult [rscObj=" + rscObj + ", childRsc=" + childRsc + "]";
        }
    }

    static class ChildResourceData
    {
        public static final List<DeviceLayerKind> ANY_KIND = Collections.unmodifiableList(
            Arrays.asList(DeviceLayerKind.values())
        );

        private final String rscNameSuffix;
        private final List<DeviceLayerKind> skipUntilList = new ArrayList<>();
        private final Set<LayerIgnoreReason> ignoreReasons;

        ChildResourceData(String rscNameSuffixRef)
        {
            rscNameSuffix = rscNameSuffixRef;
            skipUntilList.addAll(ANY_KIND);
            ignoreReasons = Collections.emptySet();
        }

        ChildResourceData(
            String rscNameSuffixRef,
            @Nullable Set<LayerIgnoreReason> ignoreReasonsRef,
            @Nullable List<DeviceLayerKind> skipUntilListRef
        )
        {
            rscNameSuffix = rscNameSuffixRef;
            ignoreReasons = ignoreReasonsRef == null ? Collections.emptySet() : ignoreReasonsRef;
            if (skipUntilListRef == null || skipUntilListRef.isEmpty())
            {
                skipUntilList.addAll(ANY_KIND);
            }
            else
            {
                skipUntilList.addAll(skipUntilListRef);
            }
        }

        ChildResourceData(
            String rscNameSuffixRef,
            @Nullable Set<LayerIgnoreReason> ignoreReasonsRef,
            @Nullable DeviceLayerKind... skipUntilKindsRef
        )
        {
            rscNameSuffix = rscNameSuffixRef;
            ignoreReasons = ignoreReasonsRef == null ? Collections.emptySet() : ignoreReasonsRef;
            if (skipUntilKindsRef == null || skipUntilKindsRef.length == 0)
            {
                skipUntilList.addAll(ANY_KIND);
            }
            else
            {
                skipUntilList.addAll(Arrays.asList(skipUntilKindsRef));
            }
        }

        @Override
        public String toString()
        {
            return "ChildResourceData [rscNameSuffix=" + rscNameSuffix + ", skipUntilList=" + skipUntilList + "]";
        }
    }

}
