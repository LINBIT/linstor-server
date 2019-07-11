package com.linbit.linstor.layer;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.CtrlStorPoolResolveHelper;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceData;
import com.linbit.linstor.ResourceDefinitionData;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.VolumeDefinitionData;
import com.linbit.linstor.Resource.RscFlags;
import com.linbit.linstor.VolumeDefinition.VlmDfnFlags;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.layer.LayerPayload.DrbdRscDfnPayload;
import com.linbit.linstor.layer.LayerPayload.StorageVlmPayload;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscDfnData;
import com.linbit.linstor.storage.data.provider.StorageRscData;
import com.linbit.linstor.storage.interfaces.categories.resource.RscLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

@Singleton
public class CtrlLayerDataHelper
{
    static final String DFLT_ROOT_RSC_NAME_SUFFIX = "";

    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final CtrlStorPoolResolveHelper storPoolResolveHelper;
    private final DrbdLayerHelper drbdLayerHelper;
    private final LuksLayerHelper luksLayerHelper;
    private final StorageLayerHelper storageLayerHelper;
    private final NvmeLayerHelper nvmeLayerHelper;

    @Inject
    public CtrlLayerDataHelper(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        CtrlStorPoolResolveHelper storPoolResolveHelperRef,
        DrbdLayerHelper drbdLayerHelperRef,
        LuksLayerHelper luksLayerHelperRef,
        StorageLayerHelper storageLayerHelperRef,
        NvmeLayerHelper nvmeLayerHelperRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        storPoolResolveHelper = storPoolResolveHelperRef;
        drbdLayerHelper = drbdLayerHelperRef;
        luksLayerHelper = luksLayerHelperRef;
        storageLayerHelper = storageLayerHelperRef;
        nvmeLayerHelper = nvmeLayerHelperRef;
    }

    public List<DeviceLayerKind> getLayerStack(Resource rscRef)
    {
        List<DeviceLayerKind> ret = new ArrayList<>();
        try
        {
            RscLayerObject layerData = rscRef.getLayerData(apiCtx);
            while (layerData != null)
            {
                ret.add(layerData.getLayerKind());
                layerData = layerData.getFirstChild();
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return ret;
    }

    /**
     * Creates the linstor default stack, which is {@link DeviceLayerKind#DRBD} on an optional
     * {@link DeviceLayerKind#LUKS} on a {@link DeviceLayerKind#STORAGE} layer.
     * A LUKS layer is created if at least one {@link VolumeDefinition}
     * has the {@link VolumeDefinition.VlmDfnFlags#ENCRYPTED} flag set.
     * @param accCtxRef
     * @return
     */
    public List<DeviceLayerKind> createDefaultStack(AccessContext accCtxRef, Resource rscRef)
    {
        List<DeviceLayerKind> layerStack;
        try
        {
            boolean hasSwordfish = hasSwordfishKind(accCtxRef, rscRef);
            if (hasSwordfish)
            {
                layerStack = Arrays.asList(DeviceLayerKind.STORAGE);
            }
            else
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
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return layerStack;
    }

    public void ensureRequiredRscDfnLayerDataExits(
        ResourceDefinitionData rscDfn,
        String rscNameSuffix,
        LayerPayload payload
    )
        throws SQLException, ValueOutOfRangeException,
            ExhaustedPoolException, ValueInUseException
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
                DrbdRscDfnData drbdRscDfnData = rscDfn.getLayerData(
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
        VolumeDefinitionData vlmDfn,
        String rscNameSuffix,
        LayerPayload payload
    )
        throws SQLException, ValueOutOfRangeException,
            ExhaustedPoolException, ValueInUseException
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

    public void ensureStackDataExists(
        ResourceData rscRef,
        List<DeviceLayerKind> layerStackRef,
        LayerPayload payload
    )
    {
        try
        {
            List<DeviceLayerKind> layerStack;
            if (layerStackRef == null || layerStackRef.isEmpty())
            {
                layerStack = getLayerStack(rscRef);
            }
            else
            {
                layerStack = layerStackRef;
            }

            if (layerStack.isEmpty())
            {
                throw new ImplementationError("Cannot create resource with empty layer list");
            }

            RscLayerObject rootObj = null;

            List<LayerResult> currentLayerDataList = new ArrayList<>();
            currentLayerDataList.add(new LayerResult(null, "")); // root object

            for (DeviceLayerKind kind : layerStack)
            {
                AbsLayerHelper<?, ?, ?, ?> layerHelper = getLayerHelperByKind(kind);
                List<LayerResult> nextLayerData = new ArrayList<>();

                /*
                 * Having the result variable here is only a hack to be able to save the root object.
                 * For the first entry of layerStack, currentLayerDataList will have exactly one element,
                 * with exactly one entry in childRscNameSuffixes (namely "").
                 * That means, for the first device layer kind, the following nested loops will only call
                 * "ensureRscDataCreated" a single time, leaving lastResult pointing to the root object.
                 */
                LayerResult result = null;
                for (LayerResult currentData : currentLayerDataList)
                {
                    for (String rscNameSuffix : currentData.childRscNameSuffixes)
                    {
                        RscLayerObject currentRscObj = currentData.rscObj;
                        result = layerHelper.ensureRscDataCreated(
                            rscRef,
                            payload,
                            rscNameSuffix,
                            currentRscObj
                        );
                        nextLayerData.add(result);
                        if (currentRscObj != null)
                        {
                            currentRscObj.getChildren().add(result.rscObj);
                        }
                    }
                }

                currentLayerDataList.clear();
                currentLayerDataList.addAll(nextLayerData);

                if (rootObj == null)
                {
                    rootObj = result.rscObj;
                }
            }

            rscRef.setLayerData(apiCtx, rootObj);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (ExhaustedPoolException exc)
        {
            errorReporter.reportError(exc);
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_POOL_EXHAUSTED_RSC_LAYER_ID,
                    "Too many layered resources!"
                ),
                exc
            );
        }
        catch (SQLException exc)
        {
            errorReporter.reportError(exc);
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_SQL,
                    "An sql excption occured while creating layer data"
                ),
                exc
            );
        }
        catch (Exception exc)
        {
            errorReporter.reportError(exc);
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_UNKNOWN_ERROR,
                    "An excption occured while creating layer data"
                ),
                exc
            );
        }
    }

    public void resetStoragePools(ResourceData rscRef)
    {
        try
        {
            LinkedList<RscLayerObject> rscDataToProcess = new LinkedList<>();
            rscDataToProcess.add(rscRef.getLayerData(apiCtx));

            while (!rscDataToProcess.isEmpty())
            {
                RscLayerObject rscData = rscDataToProcess.removeFirst();
                getLayerHelperByKind(rscData.getLayerKind()).resetStoragePools(rscData);

                rscDataToProcess.addAll(rscData.getChildren());
            }

            ensureStackDataExists(rscRef, null, new LayerPayload());
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (SQLException exc)
        {
            errorReporter.reportError(exc);
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_SQL,
                    "An sql excption occured while creating layer data"
                ),
                exc
            );
        }
        catch (Exception exc)
        {
            errorReporter.reportError(exc);
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_UNKNOWN_ERROR,
                    "An excption occured while creating layer data"
                ),
                exc
            );
        }
    }

    StorPool getStorPool(Volume vlmRef, StorageRscData rscDataRef, LayerPayload payloadRef)
        throws AccessDeniedException, InvalidKeyException, InvalidNameException
    {
        StorPool storPool = null;

        StorageVlmPayload storageVlmPayload = payloadRef.getStorageVlmPayload(
            rscDataRef.getResourceNameSuffix(),
            vlmRef.getVolumeDefinition().getVolumeNumber().value
        );
        if (storageVlmPayload != null)
        {
            storPool = vlmRef.getResource().getAssignedNode().getStorPool(
                apiCtx,
                new StorPoolName(storageVlmPayload.storPoolName)
            );
        }

        RscLayerObject child = rscDataRef;
        RscLayerObject parent = rscDataRef.getParent();
        while (parent != null && storPool == null)
        {
            AbsLayerHelper<?, ?, ?, ?> layerHelper = getLayerHelperByKind(parent.getLayerKind());

            storPool = layerHelper.getStorPool(vlmRef, child);

            child = parent;
            parent = parent.getParent();
        }
        if (storPool == null)
        {
            ApiCallRcImpl dummyApiCallRc = new ApiCallRcImpl();
            Resource rsc = vlmRef.getResource();
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

    AbsLayerHelper<?, ?, ?, ?> getLayerHelperByKind(DeviceLayerKind kind)
    {
        AbsLayerHelper<?, ?, ?, ?> layerHelper;
        switch (kind)
        {
            case DRBD:
                layerHelper = drbdLayerHelper;
                break;
            case LUKS:
                layerHelper = luksLayerHelper;
                break;
            case NVME:
                layerHelper = nvmeLayerHelper;
                break;
            case STORAGE:
                layerHelper = storageLayerHelper;
                break;
            default:
                throw new ImplementationError("Unknown device layer kind '" + kind + "'");
        }
        return layerHelper;
    }

    private boolean needsLuksLayer(AccessContext accCtxRef, Resource rscRef)
        throws AccessDeniedException
    {
        boolean needsLuksLayer = false;
        Iterator<VolumeDefinition> iterateVolumeDefinitions = rscRef.getDefinition().iterateVolumeDfn(apiCtx);
        while (iterateVolumeDefinitions.hasNext())
        {
            VolumeDefinition vlmDfn = iterateVolumeDefinitions.next();

            if (vlmDfn.getFlags().isSet(accCtxRef, VlmDfnFlags.ENCRYPTED))
            {
                needsLuksLayer = true;
                break;
            }
        }
        return needsLuksLayer;
    }

    private boolean hasSwordfishKind(AccessContext accCtxRef, Resource rscRef)
        throws AccessDeniedException
    {
        boolean foundSwordfishKind = false;

        Iterator<VolumeDefinition> iterateVolumeDfn = rscRef.getDefinition().iterateVolumeDfn(apiCtx);
        while (iterateVolumeDfn.hasNext())
        {
            VolumeDefinition vlmDfn = iterateVolumeDfn.next();
            StorPool storPool = storPoolResolveHelper.resolveStorPool(
                accCtxRef,
                rscRef,
                vlmDfn,
                rscRef.getStateFlags().isSet(apiCtx, RscFlags.DISKLESS),
                false
            ).getValue();

            if (storPool != null)
            {
                DeviceProviderKind kind = storPool.getDeviceProviderKind();
                if (kind.equals(DeviceProviderKind.SWORDFISH_INITIATOR) ||
                    kind.equals(DeviceProviderKind.SWORDFISH_TARGET))
                {
                    foundSwordfishKind = true;
                    break;
                }
            }
        }
        return foundSwordfishKind;
    }

    private boolean isDiskAddRequested(Resource rsc)
    {
        boolean isDiskAddRequested;
        try
        {
            isDiskAddRequested = rsc.getStateFlags().isSet(apiCtx, Resource.RscFlags.DISK_ADD_REQUESTED);
        }
        catch (AccessDeniedException implError)
        {
            throw new ImplementationError(implError);
        }
        return isDiskAddRequested;
    }

    private boolean isDiskless(Resource rsc)
    {
        boolean isDiskless;
        try
        {
            isDiskless = rsc.getStateFlags().isSet(apiCtx, Resource.RscFlags.DISKLESS);
        }
        catch (AccessDeniedException implError)
        {
            throw new ImplementationError(implError);
        }
        return isDiskless;
    }

    private boolean isDiskRemoving(Resource rsc)
    {
        boolean isDiskless;
        try
        {
            isDiskless = rsc.getStateFlags().isSet(apiCtx, Resource.RscFlags.DISK_REMOVING);
        }
        catch (AccessDeniedException implError)
        {
            throw new ImplementationError(implError);
        }
        return isDiskless;
    }

    /*
     * TODO: attempt to fix the "DRBD external metadata above RAID"
     * The idea would be to use instead of List<rscNameSuffix> something like
     * List<Pair<RscNameSuffix, List<DeviceLayerKindsToSkipForThisRscData>>>
     * so that DRBD layer can specify {"_meta", [RAID]}.
     * If that works, the meta-data would be still go into one storPool (instead
     * of being split while going through the RAID layer)
     */
    static class LayerResult
    {
        private RscLayerObject rscObj;
        private List<String> childRscNameSuffixes = new ArrayList<>();

        LayerResult(RscLayerObject rscObjRef)
        {
            rscObj = rscObjRef;
            childRscNameSuffixes.add(rscObjRef.getResourceNameSuffix());
        }

        LayerResult(RscLayerObject rscObjref, List<String> childRscNameSuffixesRef)
        {
            rscObj = rscObjref;
            childRscNameSuffixes.addAll(childRscNameSuffixesRef);
        }

        LayerResult(RscLayerObject rscObjRef, String... childRscNameSuffixesRef)
        {
            rscObj = rscObjRef;
            childRscNameSuffixes.addAll(Arrays.asList(childRscNameSuffixesRef));
        }
    }
}
