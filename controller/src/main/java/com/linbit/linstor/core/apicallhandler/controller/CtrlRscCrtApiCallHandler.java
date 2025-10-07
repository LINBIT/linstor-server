package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallRcWith;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscAutoHelper.AutoHelperResult;
import com.linbit.linstor.core.apicallhandler.controller.helpers.CopySnapsHelper;
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
import com.linbit.linstor.core.objects.Resource.DiskfulBy;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.event.EventStreamClosedException;
import com.linbit.linstor.event.EventStreamTimeoutException;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;
import com.linbit.utils.PairNonNull;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscApiCallHandler.getRscDescriptionInline;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.slf4j.MDC;
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
    private final CtrlRscAutoHelper autoHelper;
    private final CopySnapsHelper copySnapHelper;

    @Inject
    public CtrlRscCrtApiCallHandler(
        ScopeRunner scopeRunnerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlRscCrtApiHelper ctrlRscCrtApiHelperRef,
        ResponseConverter responseConverterRef,
        LockGuardFactory lockGuardFactoryRef,
        FreeCapacityFetcher freeCapacityFetcherRef,
        CtrlRscAutoHelper autoHelperRef,
        CopySnapsHelper copySnapHelperRef
    )
    {
        scopeRunner = scopeRunnerRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlRscCrtApiHelper = ctrlRscCrtApiHelperRef;
        responseConverter = responseConverterRef;
        lockGuardFactory = lockGuardFactoryRef;
        freeCapacityFetcher = freeCapacityFetcherRef;
        autoHelper = autoHelperRef;
        copySnapHelper = copySnapHelperRef;
    }

    public Flux<ApiCallRc> createResource(
        List<ResourceWithPayloadApi> rscApiList,
        @Nullable DiskfulBy diskfulByRef,
        boolean copyAllSnaps,
        List<String> snapNames,
        boolean copySnapsForEvac
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
            var logContextMap = MDC.getCopyOfContextMap();
            response = freeCapacityFetcher.fetchThinFreeCapacities(nodeNames)
                .flatMapMany(thinFreeCapacities ->
                    scopeRunner
                    .fluxInTransactionalScope(
                        "Create resource",
                        lockGuardFactory.buildDeferred(
                            LockType.WRITE,
                            LockObj.NODES_MAP, LockObj.RSC_DFN_MAP, LockObj.STOR_POOL_DFN_MAP
                        ),
                        () -> createResourceInTransaction(
                            rscApiList,
                            context,
                            thinFreeCapacities,
                            diskfulByRef,
                            copyAllSnaps,
                            snapNames,
                            copySnapsForEvac
                        ),
                        logContextMap
                    )
                    .transform(responses -> responseConverter.reportingExceptions(context, responses)));
        }

        return response;
    }

    /**
     * @param rscApiList
     *     Resources to create; at least one; all must belong to the same resource definition
     * @param diskfulByRef
     * @param snapNamesToCopyRef
     * @param copyAllSnapsRef
     * @param layerStackStrListRef
     */
    private Flux<ApiCallRc> createResourceInTransaction(
        List<ResourceWithPayloadApi> rscApiList,
        ResponseContext context,
        Map<StorPool.Key, Long> thinFreeCapacities,
        @Nullable DiskfulBy diskfulByRef,
        boolean copyAllSnapsRef,
        List<String> snapNamesToCopyRef,
        boolean copySnapsForEvac
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

            PairNonNull<List<Flux<ApiCallRc>>, ApiCallRcWith<Resource>> createdRsc = ctrlRscCrtApiHelper
                .createResourceDb(
                rscapi.getNodeName(),
                rscapi.getName(),
                rscapi.getFlags(),
                rscapi.getProps(),
                rscapi.getVlmList(),
                rscWithPayloadApi.getDrbdNodeId(),
                rscWithPayloadApi.getPorts(),
                rscWithPayloadApi.getPortCount(),
                thinFreeCapacities,
                rscWithPayloadApi.getLayerStack(),
                diskfulByRef
            );

            autoFlux.addAll(createdRsc.objA);
            rscNameStrsForAutoHelper.add(rscapi.getName());
            if (createdRsc.objA.isEmpty())
            {
                deployedResources.add(createdRsc.objB.extractApiCallRc(responses));
            }
        }

        for (String rscNameStr : rscNameStrsForAutoHelper)
        {
            AutoHelperResult autoHelperResult = autoHelper.manage(responses, context, rscNameStr);
            autoFlux.add(autoHelperResult.getFlux());
        }
        ctrlTransactionHelper.commit();

        for (Resource rsc : deployedResources)
        {
            responseConverter.addWithOp(
                responses, context,
                ApiSuccessUtils.defaultRegisteredEntry(rsc.getUuid(), getRscDescriptionInline(rsc))
            );

            responses.addEntries(makeVolumeRegisteredEntries(rsc));
        }

        Flux<ApiCallRc> deploymentResponses;
        if (deployedResources.isEmpty())
        {
            deploymentResponses = Flux.empty(); // no new resources, only take-over of TIE_BREAKER
        }
        else
        {
            deploymentResponses = ctrlRscCrtApiHelper.deployResources(context, deployedResources);
        }

        return Flux.<ApiCallRc>just(responses)
            .concatWith(deploymentResponses)
            .concatWith(Flux.merge(autoFlux))
            .concatWith(
                copySnapHelper.getCopyFlux(
                    deployedResources,
                    copyAllSnapsRef,
                    snapNamesToCopyRef,
                    context,
                    copySnapsForEvac
                )
            )
            .onErrorResume(CtrlResponseUtils.DelayedApiRcException.class, ignored -> Flux.empty())
            .onErrorResume(EventStreamTimeoutException.class,
                ignored -> Flux.just(ctrlRscCrtApiHelper.makeResourceDidNotAppearMessage(context)))
            .onErrorResume(EventStreamClosedException.class,
                ignored -> Flux.just(ctrlRscCrtApiHelper.makeEventStreamDisappearedUnexpectedlyMessage(context)));
    }

    static ApiCallRc makeVolumeRegisteredEntries(Resource rsc)
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
            vlmCreatedRcEntry.putObjRef(ApiConsts.KEY_RSC_DFN, rsc.getResourceDefinition().getName().displayValue);
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
