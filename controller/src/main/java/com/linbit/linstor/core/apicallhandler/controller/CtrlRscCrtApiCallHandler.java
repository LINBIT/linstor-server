package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.Resource;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.Volume;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSuccessUtils;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.event.EventStreamClosedException;
import com.linbit.linstor.event.EventStreamTimeoutException;
import com.linbit.locks.LockGuard;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscApiCallHandler.getRscDescriptionInline;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.stream.Collectors;

import reactor.core.publisher.Flux;

@Singleton
public class CtrlRscCrtApiCallHandler
{
    private final ScopeRunner scopeRunner;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlRscCrtApiHelper ctrlRscCrtApiHelper;
    private final ResponseConverter responseConverter;
    private final ReadWriteLock nodesMapLock;
    private final ReadWriteLock rscDfnMapLock;
    private final ReadWriteLock storPoolDfnMapLock;
    private final FreeCapacityFetcher freeCapacityFetcher;

    @Inject
    public CtrlRscCrtApiCallHandler(
        ScopeRunner scopeRunnerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlRscCrtApiHelper ctrlRscCrtApiHelperRef,
        ResponseConverter responseConverterRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        @Named(CoreModule.STOR_POOL_DFN_MAP_LOCK) ReadWriteLock storPoolDfnMapLockRef,
        FreeCapacityFetcher freeCapacityFetcherRef
    )
    {
        scopeRunner = scopeRunnerRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlRscCrtApiHelper = ctrlRscCrtApiHelperRef;
        responseConverter = responseConverterRef;
        nodesMapLock = nodesMapLockRef;
        rscDfnMapLock = rscDfnMapLockRef;
        storPoolDfnMapLock = storPoolDfnMapLockRef;
        freeCapacityFetcher = freeCapacityFetcherRef;
    }

    public Flux<ApiCallRc> createResource(List<Resource.RscApi> rscApiList)
    {
        List<String> rscNames = rscApiList.stream()
            .map(Resource.RscApi::getName)
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
                .map(Resource.RscApi::getNodeName)
                .map(LinstorParsingUtils::asNodeName)
                .collect(Collectors.toSet());
            response = freeCapacityFetcher.fetchThinFreeCapacities(nodeNames)
                .flatMapMany(thinFreeCapacities ->
                    scopeRunner
                    .fluxInTransactionalScope(
                        "Create resource",
                        LockGuard.createDeferred(
                            nodesMapLock.writeLock(),
                            rscDfnMapLock.writeLock(),
                            storPoolDfnMapLock.writeLock()
                        ),
                        () -> createResourceInTransaction(rscApiList, context, thinFreeCapacities)
                    )
                    .transform(responses -> responseConverter.reportingExceptions(context, responses)));
        }

        return response;
    }

    /**
     * @param rscApiList Resources to create; at least one; all must belong to the same resource definition
     */
    private Flux<ApiCallRc> createResourceInTransaction(
        List<Resource.RscApi> rscApiList,
        ResponseContext context,
        Map<StorPool.Key, Long> thinFreeCapacities
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();

        List<Resource> deployedResources = new ArrayList<>();
        for (Resource.RscApi rscApi : rscApiList)
        {
            deployedResources.add(ctrlRscCrtApiHelper.createResourceDb(
                rscApi.getNodeName(),
                rscApi.getName(),
                rscApi.getFlags(),
                rscApi.getProps(),
                rscApi.getVlmList(),
                rscApi.getLocalRscNodeId(),
                thinFreeCapacities
            ).extractApiCallRc(responses));
        }

        ctrlTransactionHelper.commit();

        for (Resource rsc : deployedResources)
        {
            responseConverter.addWithOp(responses, context,
                ApiSuccessUtils.defaultRegisteredEntry(rsc.getUuid(), getRscDescriptionInline(rsc)));

            responses.addEntries(makeVolumeRegisteredEntries(rsc));
        }

        Flux<ApiCallRc> deploymentResponses = ctrlRscCrtApiHelper.deployResources(context, deployedResources);

        return Flux.<ApiCallRc>just(responses)
            .concatWith(deploymentResponses)
            .onErrorResume(CtrlResponseUtils.DelayedApiRcException.class, ignored -> Flux.empty())
            .onErrorResume(EventStreamTimeoutException.class,
                ignored -> Flux.just(ctrlRscCrtApiHelper.makeResourceDidNotAppearMessage(context)))
            .onErrorResume(EventStreamClosedException.class,
                ignored -> Flux.just(ctrlRscCrtApiHelper.makeEventStreamDisappearedUnexpectedlyMessage(context)));
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
                    vlm.getResource().getAssignedNode().getName().displayValue +
                    "' successfully registered"
            );
            vlmCreatedRcEntry.setDetails(
                "Volume UUID is: " + vlm.getUuid().toString()
            );
            vlmCreatedRcEntry.setReturnCode(ApiConsts.MASK_CRT | ApiConsts.MASK_VLM | ApiConsts.CREATED);
            vlmCreatedRcEntry.putObjRef(ApiConsts.KEY_NODE, rsc.getAssignedNode().getName().displayValue);
            vlmCreatedRcEntry.putObjRef(ApiConsts.KEY_RSC_DFN, rsc.getDefinition().getName().displayValue);
            vlmCreatedRcEntry.putObjRef(ApiConsts.KEY_VLM_NR, Integer.toString(vlmNr));

            responses.addEntry(vlmCreatedRcEntry);
        }
        return responses;
    }

    private ResponseContext makeRscCrtContext(List<Resource.RscApi> rscApiList, String rscNameStr)
    {
        String nodeNamesStr = rscApiList.stream()
            .map(Resource.RscApi::getNodeName)
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
