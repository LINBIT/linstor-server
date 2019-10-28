package com.linbit.linstor.layer;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.CtrlStorPoolResolveHelper;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
                layerData = layerData.getChildBySuffix("");
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
     * has the {@link VolumeDefinition.Flags#ENCRYPTED} flag set.
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
        ResourceDefinition rscDfn,
        String rscNameSuffix,
        LayerPayload payload
    )
        throws DatabaseException, ValueOutOfRangeException,
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
        VolumeDefinition vlmDfn,
        String rscNameSuffix,
        LayerPayload payload
    )
        throws DatabaseException, ValueOutOfRangeException,
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
        Resource rscRef,
        List<DeviceLayerKind> layerListRef,
        LayerPayload payload
    )
    {
        try
        {
            List<DeviceLayerKind> layerList = new ArrayList<>();
            if (layerListRef == null || layerListRef.isEmpty())
            {
                layerList = getLayerStack(rscRef);
            }
            else
            {
                layerList.addAll(layerListRef);
            }

            if (layerList.isEmpty())
            {
                throw new ImplementationError("Cannot create resource with empty layer list");
            }

            RscLayerObject rootObj = null;

            List<LayerResult> currentLayerDataList = new ArrayList<>();
            currentLayerDataList.add(new LayerResult(null)); // root object

            rootObj = ensureDataRec(rscRef, payload, layerList, new ChildResourceData(""), null);

            rscRef.setLayerData(apiCtx, rootObj);
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

    private RscLayerObject ensureDataRec(
        Resource rscRef,
        LayerPayload payloadRef,
        List<DeviceLayerKind> layerStackRef,
        ChildResourceData currentData,
        RscLayerObject parentRscObj
    ) throws InvalidKeyException, DatabaseException, ExhaustedPoolException, ValueOutOfRangeException,
        ValueInUseException, LinStorException, InvalidNameException
    {
        DeviceLayerKind currentKind = layerStackRef.get(0);
        List<DeviceLayerKind> childList = layerStackRef.subList(1, layerStackRef.size());

        RscLayerObject ret;

        if (currentData.skipUntilList.contains(currentKind))
        {
            AbsLayerHelper<?, ?, ?, ?> layerHelper = getLayerHelperByKind(currentKind);
            LayerResult result = layerHelper.ensureRscDataCreated(
                rscRef,
                payloadRef,
                currentData.rscNameSuffix,
                parentRscObj
            );

            ret = result.rscObj;

            for (ChildResourceData childRsc : result.childRsc)
            {
                result.rscObj.getChildren().add(
                    ensureDataRec(
                        rscRef,
                        payloadRef,
                        childList,
                        childRsc,
                        result.rscObj
                    )
                );
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
            errorReporter.reportError(exc);
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_UNKNOWN_ERROR,
                    "An exception occurred while creating layer data"
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

            if (vlmDfn.getFlags().isSet(accCtxRef, VolumeDefinition.Flags.ENCRYPTED))
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
                rscRef.getStateFlags().isSet(apiCtx, Resource.Flags.DISKLESS),
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
            isDiskAddRequested = rsc.getStateFlags().isSet(apiCtx, Resource.Flags.DISK_ADD_REQUESTED);
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
            isDiskless = rsc.getStateFlags().isSet(apiCtx, Resource.Flags.DISKLESS);
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
            isDiskless = rsc.getStateFlags().isSet(apiCtx, Resource.Flags.DISK_REMOVING);
        }
        catch (AccessDeniedException implError)
        {
            throw new ImplementationError(implError);
        }
        return isDiskless;
    }

    /**
     * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
     */
    static class LayerResult
    {
        private RscLayerObject rscObj;
        private List<ChildResourceData> childRsc = new ArrayList<>();

        LayerResult(RscLayerObject rscObjRef)
        {
            rscObj = rscObjRef;
            childRsc.add(new ChildResourceData(""));
        }

        LayerResult(RscLayerObject rscObjref, List<ChildResourceData> childRscListRef)
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

        ChildResourceData(String rscNameSuffixRef)
        {
            rscNameSuffix = rscNameSuffixRef;
            skipUntilList.addAll(ANY_KIND);
        }

        ChildResourceData(String rscNameSuffixRef, List<DeviceLayerKind> skipUntilListRef)
        {
            rscNameSuffix = rscNameSuffixRef;
            skipUntilList.addAll(skipUntilListRef);
        }

        ChildResourceData(String rscNameSuffixRef, DeviceLayerKind... skipUntilKindsRef)
        {
            rscNameSuffix = rscNameSuffixRef;
            skipUntilList.addAll(Arrays.asList(skipUntilKindsRef));
        }

        @Override
        public String toString()
        {
            return "ChildResourceData [rscNameSuffix=" + rscNameSuffix + ", skipUntilList=" + skipUntilList + "]";
        }
    }

}
