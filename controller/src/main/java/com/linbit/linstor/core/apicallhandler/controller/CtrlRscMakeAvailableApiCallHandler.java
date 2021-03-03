package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.pojo.AutoSelectFilterPojo;
import com.linbit.linstor.api.pojo.ResourceWithPayloadPojo;
import com.linbit.linstor.api.pojo.RscPojo;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.autoplacer.Autoplacer;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.apis.ResourceWithPayloadApi;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.utils.layer.LayerRscUtils;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import reactor.core.publisher.Flux;

@Singleton
public class CtrlRscMakeAvailableApiCallHandler
{
    private final ScopeRunner scopeRunner;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final ResponseConverter responseConverter;
    private final FreeCapacityFetcher freeCapacityFetcher;
    private final LockGuardFactory lockGuardFactory;
    private final Provider<AccessContext> peerCtxProvider;
    private final CtrlRscCrtApiCallHandler ctrlRscCrtApiCallHandler;
    private final CtrlApiDataLoader dataLoader;
    private final Autoplacer autoplacer;

    @Inject
    public CtrlRscMakeAvailableApiCallHandler(
        ScopeRunner scopeRunnerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        ResponseConverter responseConverterRef,
        LockGuardFactory lockGuardFactoryRef,
        FreeCapacityFetcher freeCapacityFetcherRef,
        @PeerContext Provider<AccessContext> peerCtxProviderRef,
        CtrlRscCrtApiCallHandler ctrlRscCrtApiCallHandlerRef,
        CtrlApiDataLoader dataLoaderRef,
        Autoplacer autoplacerRef
    )
    {
        scopeRunner = scopeRunnerRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        responseConverter = responseConverterRef;
        lockGuardFactory = lockGuardFactoryRef;
        freeCapacityFetcher = freeCapacityFetcherRef;
        peerCtxProvider = peerCtxProviderRef;
        ctrlRscCrtApiCallHandler = ctrlRscCrtApiCallHandlerRef;
        dataLoader = dataLoaderRef;
        autoplacer = autoplacerRef;
    }

    public Flux<ApiCallRc> makeResourceAvailable(
        String nodeNameRef,
        String rscNameRef,
        List<String> layerStackRef,
        Boolean diskfulRef
    )
    {
        ResponseContext context = makeContext(nodeNameRef, rscNameRef);

        return scopeRunner.fluxInTransactionalScope(
                "Make resource available",
                lockGuardFactory.buildDeferred(
                    LockType.WRITE,
                    LockObj.NODES_MAP,
                    LockObj.RSC_DFN_MAP,
                    LockObj.STOR_POOL_DFN_MAP
                ),
                () -> makeRscAvailableInTransaction(
                    nodeNameRef,
                    rscNameRef,
                    layerStackRef,
                    diskfulRef,
                    context
                )
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> makeRscAvailableInTransaction(
        String nodeNameRef,
        String rscNameRef,
        List<String> layerStackRef,
        Boolean diskfulRef,
        ResponseContext contextRef
    )
    {
        Flux<ApiCallRc> flux;

        ResourceDefinition rscDfn = dataLoader.loadRscDfn(rscNameRef, true);
        Resource rsc = dataLoader.loadRsc(nodeNameRef, rscNameRef, false);
        List<DeviceLayerKind> layerStack = getLayerStack(layerStackRef, rscDfn);

        if (rsc != null)
        {
            /*
             * For now, we can only perform some basic checks if the wanted resource looks like the existing one.
             * If not, response with an error RC.
             */
            if (!layerStack.isEmpty() && !layerStack.equals(getDeployedLayerStack(rsc)))
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_INVLD_LAYER_STACK,
                        "Layerstack of deployed resource does not match"
                    )
                );
            }

            /*
             * TODO: if this gets rebased with shared SP, activate the resource if inactive and is allowed to be
             * activated
             */
            // if (isFlagSet(rsc, Resource.Flags.INACTIVE) && !isFlagSet(rsc, Resource.Flags.INACTIVE_PERMANENTLY))
            // {
            // // activate rsc
            // }

            flux = Flux.just(
                ApiCallRcImpl.singleApiCallRc(ApiConsts.MASK_SUCCESS, "Resource already deployed as requested")
            );
        }
        else
        {
            AutoSelectFilterPojo autoSelect = null;
            long rscFlags = 0;
            boolean disklessForErrorMsg = false;

            if (layerStack.contains(DeviceLayerKind.DRBD))
            {
                if ((diskfulRef == null || !diskfulRef) && hasDrbdDiskfulPeer(rscDfn))
                {
                    // we can create a DRBD diskless resource
                    autoSelect = createAutoSelectConfig(
                        nodeNameRef,
                        layerStack,
                        Resource.Flags.DRBD_DISKLESS
                    );

                    rscFlags = Resource.Flags.DRBD_DISKLESS.flagValue;
                    disklessForErrorMsg = true;
                }
                else
                {
                    /*
                     * No diskful peer (or forced diskful). "make resource available" is interpreted in this case as
                     * creating the first
                     * diskful resource. However, this might still mean that other layers like NVMe are involved
                     */
                    if (layerStack.contains(DeviceLayerKind.NVME) || layerStack.contains(DeviceLayerKind.OPENFLEX))
                    {
                        if (hasNvmeTarget(rscDfn))
                        {
                            // we want to connect as initiator
                            autoSelect = createAutoSelectConfig(nodeNameRef, layerStack, Resource.Flags.NVME_INITIATOR);
                            rscFlags = Resource.Flags.NVME_INITIATOR.flagValue;
                            disklessForErrorMsg = true;
                        }
                    }
                    else
                    {
                        // default diskful DRBD setup with the given layers
                        autoSelect = createAutoSelectConfig(nodeNameRef, layerStack, null);
                        rscFlags = 0;
                        disklessForErrorMsg = false;
                    }
                }
            }

            if (autoSelect == null)
            {
                // TODO: this will change once shared SP is merged into master

                // default diskful setup with the given layers
                autoSelect = createAutoSelectConfig(nodeNameRef, layerStack, null);
                rscFlags = 0;
                disklessForErrorMsg = false;
            }

            Set<StorPool> storPoolSet = autoplacer.autoPlace(
                AutoSelectFilterPojo.merge(
                    autoSelect,
                    rscDfn.getResourceGroup().getAutoPlaceConfig().getApiData()
                ),
                rscDfn,
                CtrlRscAutoPlaceApiCallHandler.calculateResourceDefinitionSize(rscDfn, peerCtxProvider.get())
            );

            StorPool sp = getStorPoolOrFail(storPoolSet, nodeNameRef, disklessForErrorMsg);
            ResourceWithPayloadApi createRscPojo = new ResourceWithPayloadPojo(
                new RscPojo(
                    rscNameRef,
                    nodeNameRef,
                    rscFlags,
                    Collections.singletonMap(
                        ApiConsts.KEY_STOR_POOL_NAME,
                        sp.getName().displayValue
                        )
                    ),
                layerStack.stream().map(DeviceLayerKind::name).collect(Collectors.toList()),
                null
                );
            flux = ctrlRscCrtApiCallHandler.createResource(Collections.singletonList(createRscPojo));
            ctrlTransactionHelper.commit();
        }

        return flux;
    }

    private boolean hasDrbdDiskfulPeer(ResourceDefinition rscDfnRef)
    {
        return hasPeerWithoutFlag(rscDfnRef, Resource.Flags.DRBD_DISKLESS);
    }

    private boolean hasNvmeTarget(ResourceDefinition rscDfnRef)
    {
        return hasPeerWithoutFlag(rscDfnRef, Resource.Flags.NVME_INITIATOR);
    }

    private boolean hasPeerWithoutFlag(ResourceDefinition rscDfn, Resource.Flags flag)
    {
        Iterator<Resource> rscIt = getRscIter(rscDfn);
        boolean foundPeer = false;
        while (rscIt.hasNext())
        {
            Resource peerRsc = rscIt.next();
            if (!isFlagSet(peerRsc, flag))
            {
                foundPeer = true;
                break;
            }
        }
        return foundPeer;
    }

    private AutoSelectFilterPojo createAutoSelectConfig(
        String nodeName,
        List<DeviceLayerKind> layerStack,
        Resource.Flags disklessFlag
    )
    {
        return new AutoSelectFilterPojo(
            0,
            1,
            Collections.singletonList(nodeName),
            null,
            null,
            null,
            null,
            null,
            layerStack,
            null,
            null,
            null,
            disklessFlag == null ? null : disklessFlag.name()
        );
    }

    private StorPool getStorPoolOrFail(Set<StorPool> storPoolSetRef, String nodeNameRef, boolean disklessRef)
    {
        if (storPoolSetRef == null)
        {
            throw failNoStorPoolFound(nodeNameRef, disklessRef);
        }
        if (storPoolSetRef.isEmpty())
        {
            throw failNoStorPoolFound(nodeNameRef, disklessRef);
        }
        if (storPoolSetRef.size() != 1)
        {
            throw new ImplementationError(
                "Only one storPool expected. got: " + storPoolSetRef.size() + ". " + storPoolSetRef
            );
        }
        return storPoolSetRef.iterator().next();
    }

    private ApiRcException failNoStorPoolFound(String nodeName, boolean diskless)
    {
        return new ApiRcException(
            ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_NOT_FOUND_STOR_POOL,
                "Autoplacer could not find " + (diskless ? "diskless" : "diskful") + " stor pool on node " + nodeName +
                    " matching resource-groups autoplace-settings"
            )
        );
    }


    private List<DeviceLayerKind> getDeployedLayerStack(Resource rscRef)
    {
        List<DeviceLayerKind> layerStack;
        try
        {
            layerStack = LayerRscUtils.getLayerStack(rscRef, peerCtxProvider.get());
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "getting layer stack of resource " + CtrlRscApiCallHandler.getRscDescription(rscRef),
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        return layerStack;
    }

    private List<DeviceLayerKind> getLayerStack(List<String> layerStackStr, ResourceDefinition rscDfnRef)
    {
        List<DeviceLayerKind> layerStack;
        if (layerStackStr == null || layerStackStr.isEmpty())
        {
            try
            {
                layerStack = rscDfnRef.getLayerStack(peerCtxProvider.get());
            }
            catch (AccessDeniedException exc)
            {
                throw new ApiAccessDeniedException(
                    exc,
                    "accessing layer list of rscDfn " + rscDfnRef.getName(),
                    ApiConsts.FAIL_ACC_DENIED_RSC
                );
            }
        }
        else
        {
            layerStack = LinstorParsingUtils.asDeviceLayerKind(layerStackStr);
        }

        if (layerStack == null || layerStack.isEmpty())
        {
            layerStack = Arrays.asList(DeviceLayerKind.DRBD, DeviceLayerKind.STORAGE);
        }
        return layerStack;
    }

    private Iterator<Resource> getRscIter(ResourceDefinition rscDfnRef)
    {
        Iterator<Resource> rscIt;
        try
        {
            rscIt = rscDfnRef.iterateResource(peerCtxProvider.get());
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "accessing resources of rscDfn " + rscDfnRef.getName(),
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        return rscIt;
    }

    private boolean isFlagSet(Resource rsc, Resource.Flags... flags)
    {
        boolean isSet;
        try
        {
            isSet = rsc.getStateFlags().isSet(peerCtxProvider.get(), flags);
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(exc, "checking resource flags", ApiConsts.FAIL_ACC_DENIED_RSC);
        }
        return isSet;
    }

    private ResponseContext makeContext(String nodeName, String rscName)
    {
        Map<String, String> objRefs = new TreeMap<>();
        objRefs.put(ApiConsts.KEY_NODE, nodeName);
        objRefs.put(ApiConsts.KEY_RSC_DFN, rscName);

        return new ResponseContext(
            ApiOperation.makeRegisterOperation(),
            "Node: " + nodeName + ", Resource: '" + rscName + "'",
            "resource '" + rscName + "' on node " + nodeName + "",
            ApiConsts.MASK_RSC,
            objRefs
        );
    }
}
