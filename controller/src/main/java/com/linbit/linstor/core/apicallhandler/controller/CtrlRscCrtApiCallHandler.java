package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscAutoHelper.AutoHelperResult;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSuccessUtils;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.apis.ResourceApi;
import com.linbit.linstor.core.apis.ResourceWithPayloadApi;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.event.EventStreamClosedException;
import com.linbit.linstor.event.EventStreamTimeoutException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscObject.DrbdRscFlags;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.utils.LayerUtils;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscApiCallHandler.getRscDescriptionInline;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import reactor.core.publisher.Flux;

@Singleton
public class CtrlRscCrtApiCallHandler
{
    private final ScopeRunner scopeRunner;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlRscCrtApiHelper ctrlRscCrtApiHelper;
    private final ResponseConverter responseConverter;
    private final FreeCapacityFetcher freeCapacityFetcher;
    private final LockGuardFactory lockGuardFactory;
    private final Provider<AccessContext> peerCtxProvider;
    private final CtrlRscAutoHelper autoHelper;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final CtrlRscToggleDiskApiCallHandler toggleDiskHelper;

    @Inject
    public CtrlRscCrtApiCallHandler(
        ScopeRunner scopeRunnerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlRscCrtApiHelper ctrlRscCrtApiHelperRef,
        ResponseConverter responseConverterRef,
        LockGuardFactory lockGuardFactoryRef,
        FreeCapacityFetcher freeCapacityFetcherRef,
        @PeerContext Provider<AccessContext> peerCtxProviderRef,
        CtrlRscAutoHelper autoHelperRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        CtrlRscToggleDiskApiCallHandler toggleDiskHelperRef
    )
    {
        scopeRunner = scopeRunnerRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlRscCrtApiHelper = ctrlRscCrtApiHelperRef;
        responseConverter = responseConverterRef;
        lockGuardFactory = lockGuardFactoryRef;
        freeCapacityFetcher = freeCapacityFetcherRef;
        peerCtxProvider = peerCtxProviderRef;
        autoHelper = autoHelperRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        toggleDiskHelper = toggleDiskHelperRef;
    }

    public Flux<ApiCallRc> createResource(
        List<ResourceWithPayloadApi> rscApiList
    )
    {
        List<String> rscNames = rscApiList.stream()
            .map(rscWithPayLoad -> rscWithPayLoad.getRscApi().getName())
            .sorted()
            .distinct()
            .collect(Collectors.toList());

        Flux<ApiCallRc> response;
        if (rscNames.isEmpty())
        {
            response = Flux.just(ApiCallRcImpl.singletonApiCallRc(ApiCallRcImpl.simpleEntry(
                ApiConsts.WARN_NOT_FOUND, "No resources specified"
            )));
        }
        else
        {
            if (rscNames.size() > 1)
            {
                throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_RSC_NAME,
                    "All resources to be created must belong to the same resource definition"
                ));
            }

            ResponseContext context = makeRscCrtContext(rscApiList, rscNames.get(0));

            Set<NodeName> nodeNames = rscApiList.stream()
                .map(rscWithPayload -> rscWithPayload.getRscApi().getNodeName())
                .map(LinstorParsingUtils::asNodeName)
                .collect(Collectors.toSet());
            response = freeCapacityFetcher.fetchThinFreeCapacities(nodeNames)
                .flatMapMany(thinFreeCapacities ->
                    scopeRunner
                    .fluxInTransactionalScope(
                        "Create resource",
                        lockGuardFactory.buildDeferred(
                            LockType.WRITE,
                            LockObj.NODES_MAP, LockObj.RSC_DFN_MAP, LockObj.STOR_POOL_DFN_MAP
                        ),
                        () -> createResourceInTransaction(rscApiList, context, thinFreeCapacities)
                    )
                    .transform(responses -> responseConverter.reportingExceptions(context, responses)));
        }

        return response;
    }

    /**
     * @param rscApiList Resources to create; at least one; all must belong to the same resource definition
     * @param layerStackStrListRef
     */
    private Flux<ApiCallRc> createResourceInTransaction(
        List<ResourceWithPayloadApi> rscApiList,
        ResponseContext context,
        Map<StorPool.Key, Long> thinFreeCapacities
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();

        Set<String> rscNameStrsForAutoHelper = new TreeSet<>();
        Set<Resource> deployedResources = new TreeSet<>();
        List<Flux<ApiCallRc>> autoFlux = new ArrayList<>();
        // create all resources from the user
        for (ResourceWithPayloadApi rscWithPayloadApi : rscApiList)
        {
            ResourceApi rscapi = rscWithPayloadApi.getRscApi();

            Resource tiebreaker = autoHelper.getTiebreakerResource(rscapi.getNodeName(), rscapi.getName());
            if (tiebreaker == null)
            {
                deployedResources.add(
                    ctrlRscCrtApiHelper.createResourceDb(
                        rscapi.getNodeName(),
                        rscapi.getName(),
                        rscapi.getFlags(),
                        rscapi.getProps(),
                        rscapi.getVlmList(),
                        rscWithPayloadApi.getDrbdNodeId(),
                        thinFreeCapacities,
                        rscWithPayloadApi.getLayerStack()
                    ).extractApiCallRc(responses)
                );
            }
            else
            {
                autoHelper.removeTiebreakerFlag(tiebreaker);
                boolean isDiskless =
                    FlagsHelper.isFlagEnabled(rscapi.getFlags(), Resource.Flags.DISKLESS) || // needed for compatibility
                    FlagsHelper.isFlagEnabled(rscapi.getFlags(), Resource.Flags.DRBD_DISKLESS) ||
                    FlagsHelper.isFlagEnabled(rscapi.getFlags(), Resource.Flags.NVME_INITIATOR);
                if (!isDiskless)
                {
                    // target resource is diskful
                    autoFlux.add(
                        toggleDiskHelper.resourceToggleDisk(
                            rscapi.getNodeName(),
                            rscapi.getName(),
                            rscapi.getProps().get(ApiConsts.KEY_STOR_POOL_NAME),
                            null,
                            false
                        )
                    );
                }
                else
                {
                    // target resource is diskless.
                    NodeName tiebreakerNodeName = tiebreaker.getNode().getName();
                    autoFlux.add(
                        ctrlSatelliteUpdateCaller.updateSatellites(
                            tiebreaker.getDefinition(),
                            Flux.empty() // if failed, there is no need for the retry-task to wait for readyState
                            // this is only true as long as there is no other flux concatenated after readyResponses
                        )
                        .transform(
                            updateResponses -> CtrlResponseUtils.combineResponses(
                                updateResponses,
                                LinstorParsingUtils.asRscName(rscapi.getName()),
                                Collections.singleton(tiebreakerNodeName),
                                "Removed TIE_BREAKER flag from resource {1} on {0}",
                                "Update of resource {1} on '" + tiebreakerNodeName + "' applied on node {0}"
                            )
                        )
                    );
                }
            }
            rscNameStrsForAutoHelper.add(rscapi.getName());
        }

        for (String rscNameStr : rscNameStrsForAutoHelper)
        {
            AutoHelperResult autoHelperResult = autoHelper.manage(responses, context, rscNameStr);
            autoFlux.add(autoHelperResult.getFlux());
        }
        ctrlTransactionHelper.commit();

        for (Resource rsc : deployedResources)
        {
            responseConverter.addWithOp(responses, context,
                ApiSuccessUtils.defaultRegisteredEntry(rsc.getUuid(), getRscDescriptionInline(rsc)));

            responses.addEntries(makeVolumeRegisteredEntries(rsc));
        }

        Flux<ApiCallRc> deploymentResponses;
        if (deployedResources.isEmpty())
        {
            deploymentResponses = Flux.empty();// no new resources, only take-over of TIE_BREAKER
        }
        else
        {
            deploymentResponses = ctrlRscCrtApiHelper.deployResources(context, deployedResources);
        }

        return Flux.<ApiCallRc>just(responses)
            .concatWith(deploymentResponses)
            .concatWith(setInitialized(deployedResources, context))
            .concatWith(Flux.merge(autoFlux))
            .onErrorResume(CtrlResponseUtils.DelayedApiRcException.class, ignored -> Flux.empty())
            .onErrorResume(EventStreamTimeoutException.class,
                ignored -> Flux.just(ctrlRscCrtApiHelper.makeResourceDidNotAppearMessage(context)))
            .onErrorResume(EventStreamClosedException.class,
                ignored -> Flux.just(ctrlRscCrtApiHelper.makeEventStreamDisappearedUnexpectedlyMessage(context)));
    }

    private Flux<ApiCallRc> setInitialized(Set<Resource> deployedResourcesRef, ResponseContext context)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Create resource",
                lockGuardFactory.buildDeferred(
                    LockType.WRITE,
                    LockObj.NODES_MAP, LockObj.RSC_DFN_MAP, LockObj.STOR_POOL_DFN_MAP
                ),
                () -> setInitializedInTransaction(deployedResourcesRef, context)
            );
    }

    private Flux<ApiCallRc> setInitializedInTransaction(Set<Resource> deployedResourcesRef, ResponseContext contextRef)
    {
        try
        {
            AccessContext peerCtx = peerCtxProvider.get();
            for (Resource rsc : deployedResourcesRef)
            {
                List<AbsRscLayerObject<Resource>> drbdRscList = LayerUtils
                    .getChildLayerDataByKind(rsc.getLayerData(peerCtx), DeviceLayerKind.DRBD);
                for (AbsRscLayerObject<Resource> drbdRsc : drbdRscList)
                {
                    ((DrbdRscData<Resource>) drbdRsc).getFlags().enableFlags(peerCtx, DrbdRscFlags.INITIALIZED);
                }
            }
            ctrlTransactionHelper.commit();
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(exc, "setting resource to initialized", ApiConsts.FAIL_ACC_DENIED_RSC);
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
        return Flux.empty();
    }

    private ApiCallRc makeVolumeRegisteredEntries(Resource rsc)
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        Iterator<Volume> vlmIt = rsc.iterateVolumes();
        while (vlmIt.hasNext())
        {
            Volume vlm = vlmIt.next();
            int vlmNr = vlm.getVolumeDefinition().getVolumeNumber().value;

            ApiCallRcImpl.ApiCallRcEntry vlmCreatedRcEntry = new ApiCallRcImpl.ApiCallRcEntry();
            vlmCreatedRcEntry.setMessage(
                "Volume with number '" + vlmNr + "' on resource '" +
                    vlm.getResourceDefinition().getName().displayValue + "' on node '" +
                    vlm.getAbsResource().getNode().getName().displayValue +
                    "' successfully registered"
            );
            vlmCreatedRcEntry.setDetails(
                "Volume UUID is: " + vlm.getUuid().toString()
            );
            vlmCreatedRcEntry.setReturnCode(ApiConsts.MASK_CRT | ApiConsts.MASK_VLM | ApiConsts.CREATED);
            vlmCreatedRcEntry.putObjRef(ApiConsts.KEY_NODE, rsc.getNode().getName().displayValue);
            vlmCreatedRcEntry.putObjRef(ApiConsts.KEY_RSC_DFN, rsc.getDefinition().getName().displayValue);
            vlmCreatedRcEntry.putObjRef(ApiConsts.KEY_VLM_NR, Integer.toString(vlmNr));

            responses.addEntry(vlmCreatedRcEntry);
        }
        return responses;
    }

    private ResponseContext makeRscCrtContext(List<ResourceWithPayloadApi> rscApiListRef, String rscNameStr)
    {
        String nodeNamesStr = rscApiListRef.stream()
            .map(rscWithPayload -> rscWithPayload.getRscApi().getNodeName())
            .map(nodeNameStr -> "'" + nodeNameStr + "'")
            .collect(Collectors.joining(", "));

        Map<String, String> objRefs = new TreeMap<>();
        objRefs.put(ApiConsts.KEY_RSC_DFN, rscNameStr);

        return new ResponseContext(
            ApiOperation.makeRegisterOperation(),
            "Node(s): " + nodeNamesStr + ", Resource: '" + rscNameStr + "'",
            "resource '" + rscNameStr + "' on node(s) " + nodeNamesStr + "",
            ApiConsts.MASK_RSC,
            objRefs
        );
    }
}
