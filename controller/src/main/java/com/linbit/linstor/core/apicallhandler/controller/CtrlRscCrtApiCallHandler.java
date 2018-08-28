package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.InvalidNameException;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceData;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.Volume;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiSuccessUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.apicallhandler.response.ResponseUtils;
import com.linbit.linstor.event.EventStreamClosedException;
import com.linbit.linstor.event.EventStreamTimeoutException;
import com.linbit.linstor.event.EventWaiter;
import com.linbit.linstor.event.ObjectIdentifier;
import com.linbit.linstor.event.common.ResourceStateEvent;
import com.linbit.linstor.event.common.UsageState;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.locks.LockGuard;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscApiCallHandler.getRscDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscApiCallHandler.makeRscContext;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscDfnApiCallHandler.getRscDfnDescriptionInline;

@Singleton
public class CtrlRscCrtApiCallHandler
{
    private final ScopeRunner scopeRunner;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlRscApiCallHandler ctrlRscApiCallHandler;
    private final EventWaiter eventWaiter;
    private final ResourceStateEvent resourceStateEvent;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final ResponseConverter responseConverter;
    private final ReadWriteLock nodesMapLock;
    private final ReadWriteLock rscDfnMapLock;
    private final Provider<AccessContext> peerAccCtx;

    @Inject
    public CtrlRscCrtApiCallHandler(
        ScopeRunner scopeRunnerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlRscApiCallHandler ctrlRscApiCallHandlerRef,
        EventWaiter eventWaiterRef, ResourceStateEvent resourceStateEventRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        ResponseConverter responseConverterRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef
    )
    {
        scopeRunner = scopeRunnerRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlRscApiCallHandler = ctrlRscApiCallHandlerRef;
        eventWaiter = eventWaiterRef;
        resourceStateEvent = resourceStateEventRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        responseConverter = responseConverterRef;
        nodesMapLock = nodesMapLockRef;
        rscDfnMapLock = rscDfnMapLockRef;
        peerAccCtx = peerAccCtxRef;
    }

    public Flux<ApiCallRc> createResource(
        String nodeNameStr,
        String rscNameStr,
        List<String> flagList,
        Map<String, String> rscPropsMap,
        List<Volume.VlmApi> vlmApiList,
        Integer nodeIdInt
    )
    {
        ResponseContext context = makeRscContext(
            ApiOperation.makeRegisterOperation(),
            nodeNameStr,
            rscNameStr
        );

        return scopeRunner
            .fluxInTransactionalScope(
                LockGuard.createDeferred(
                    nodesMapLock.writeLock(),
                    rscDfnMapLock.writeLock()
                ),
                () -> createResourceInTransaction(
                    nodeNameStr,
                    rscNameStr,
                    flagList,
                    rscPropsMap,
                    vlmApiList,
                    nodeIdInt,
                    context
                )
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> createResourceInTransaction(
        String nodeNameStr,
        String rscNameStr,
        List<String> flagList,
        Map<String, String> rscPropsMap,
        List<Volume.VlmApi> vlmApiList,
        Integer nodeIdInt,
        ResponseContext context
    )
        throws InvalidNameException
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();

        ResourceData rsc = ctrlRscApiCallHandler.createResourceDb(
            nodeNameStr,
            rscNameStr,
            flagList,
            rscPropsMap,
            vlmApiList,
            nodeIdInt
        ).extractApiCallRc(responses);

        ctrlTransactionHelper.commit();

        responseConverter.addWithOp(responses, context,
            ApiSuccessUtils.defaultRegisteredEntry(rsc.getUuid(), getRscDescriptionInline(rsc)));

        responses.addEntries(makeVolumeRegisteredEntries(rsc));

        NodeName nodeName = rsc.getAssignedNode().getName();
        ResourceName rscName = rsc.getDefinition().getName();

        Flux<ApiCallRc> satelliteUpdateResponses = ctrlSatelliteUpdateCaller.updateSatellites(rsc)
            .transform(updateResponses -> ResponseUtils.translateDeploymentSuccess(
                updateResponses,
                nodeName,
                "Created resource on '%s'",
                "Added peer on '%s' to resource on '" + nodeName.displayValue + "'"
            ));

        Mono<ApiCallRc> resourceReadyResponses;
        if (rsc.getVolumeCount() == 0)
        {
            // Do DRBD resource is created when no volumes are present, so do not wait for it to be ready
            resourceReadyResponses = Mono.just(makeNoVolumesMessage());
        }
        else if (allDiskless(rsc.getDefinition()))
        {
            resourceReadyResponses = Mono.just(makeAllDisklessMessage());
        }
        else
        {
            resourceReadyResponses = eventWaiter
                .waitForStream(
                    resourceStateEvent.get(),
                    ObjectIdentifier.resource(nodeName, rscName)
                )
                .skipUntil(UsageState::getResourceReady)
                .next()
                .thenReturn(makeResourceReadyMessage(context, nodeName, rscName));
        }

        return Flux
            .<ApiCallRc>just(responses)
            .concatWith(satelliteUpdateResponses)
            .concatWith(resourceReadyResponses)
            .onErrorResume(CtrlSatelliteUpdateCaller.DelayedApiRcException.class, ignored -> Flux.empty())
            .onErrorResume(EventStreamTimeoutException.class,
                ignored -> Flux.just(makeResourceDidNotAppearMessage(context)))
            .onErrorResume(EventStreamClosedException.class,
                ignored -> Flux.just(makeEventStreamDisappearedUnexpectedlyMessage(context)));
    }

    private ApiCallRc makeVolumeRegisteredEntries(ResourceData rsc)
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

    private boolean allDiskless(ResourceDefinition rscDfn)
    {
        boolean allDiskless = true;
        try
        {
            Iterator<Resource> rscIter = rscDfn.iterateResource(peerAccCtx.get());
            while (rscIter.hasNext())
            {
                if (!rscIter.next().getStateFlags().isSet(peerAccCtx.get(), Resource.RscFlags.DISKLESS))
                {
                    allDiskless = false;
                }
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "check diskless state of " + getRscDfnDescriptionInline(rscDfn),
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }
        return allDiskless;
    }

    private ApiCallRc makeResourceReadyMessage(
        ResponseContext context,
        NodeName nodeName,
        ResourceName rscName
    )
    {
        return ApiCallRcImpl.singletonApiCallRc(responseConverter.addContext(ApiCallRcImpl.simpleEntry(
            ApiConsts.CREATED,
            "Resource ready"
        ), context, true));
    }

    private ApiCallRcImpl makeNoVolumesMessage()
    {
        return ApiCallRcImpl.singletonApiCallRc(ApiCallRcImpl.simpleEntry(
            ApiConsts.WARN_NOT_FOUND,
            "No volumes have been defined"
        ));
    }

    private ApiCallRcImpl makeAllDisklessMessage()
    {
        return ApiCallRcImpl.singletonApiCallRc(ApiCallRcImpl.simpleEntry(
            ApiConsts.WARN_ALL_DISKLESS,
            "Resource unusable because it is diskless on all its nodes"
        ));
    }

    private ApiCallRc makeResourceDidNotAppearMessage(ResponseContext context)
    {
        return ApiCallRcImpl.singletonApiCallRc(responseConverter.addContext(ApiCallRcImpl.simpleEntry(
            ApiConsts.FAIL_UNKNOWN_ERROR,
            "Deployed resource did not appear"
        ), context, true));
    }

    private ApiCallRc makeEventStreamDisappearedUnexpectedlyMessage(ResponseContext context)
    {
        return ApiCallRcImpl.singletonApiCallRc(responseConverter.addContext(ApiCallRcImpl.simpleEntry(
            ApiConsts.FAIL_UNKNOWN_ERROR,
            "Resource disappeared while waiting for it to be ready"
        ), context, true));
    }
}
