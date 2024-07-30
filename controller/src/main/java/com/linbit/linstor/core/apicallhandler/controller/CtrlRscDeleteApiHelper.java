package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscAutoHelper.AutoHelperContext;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.satellitestate.SatelliteResourceState;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.tasks.RetryResourcesTask;
import com.linbit.linstor.tasks.ScheduleBackupService;
import com.linbit.locks.LockGuard;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscApiCallHandler.getRscDescription;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscApiCallHandler.getRscDescriptionInline;
import static com.linbit.utils.StringUtils.firstLetterCaps;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.slf4j.MDC;
import reactor.core.publisher.Flux;

@Singleton
public class CtrlRscDeleteApiHelper
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final ScopeRunner scopeRunner;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final Provider<AccessContext> peerAccCtx;
    private final LockGuardFactory lockGuardFactory;
    private final ScheduleBackupService scheduleService;
    private final RetryResourcesTask retryRscTask;
    private final Provider<CtrlRscAutoHelper> rscAutoHelperProvider;

    @Inject
    public CtrlRscDeleteApiHelper(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        ScopeRunner scopeRunnerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        LockGuardFactory lockGuardFactoryRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        ScheduleBackupService scheduleServiceRef,
        RetryResourcesTask retryRscTaskRef,
        Provider<CtrlRscAutoHelper> rscAutoHelperProviderRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        scopeRunner = scopeRunnerRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        lockGuardFactory = lockGuardFactoryRef;
        peerAccCtx = peerAccCtxRef;
        scheduleService = scheduleServiceRef;
        retryRscTask = retryRscTaskRef;
        rscAutoHelperProvider = rscAutoHelperProviderRef;
    }

    public void markDeletedWithVolumes(Resource rsc)
    {
        try
        {
            ResourceDefinition rscDfn = rsc.getResourceDefinition();
            rsc.markDeleted(peerAccCtx.get());
            if (rscDfn.getNotDeletedDiskfulCount(apiCtx) == 0)
            {
                scheduleService.removeTasks(rscDfn);
            }

            Iterator<Volume> volumesIterator = rsc.iterateVolumes();
            while (volumesIterator.hasNext())
            {
                Volume vlm = volumesIterator.next();
                vlm.markDeleted(peerAccCtx.get());
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "mark " + getRscDescription(rsc) + " as deleted",
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
    }

    public void markDrbdDeletedWithVolumes(Resource rsc)
    {
        try
        {
            rsc.markDrbdDeleted(peerAccCtx.get());
            Iterator<Volume> volumesIterator = rsc.iterateVolumes();
            while (volumesIterator.hasNext())
            {
                Volume vlm = volumesIterator.next();
                vlm.markDrbdDeleted(peerAccCtx.get());
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "mark " + getRscDescription(rsc) + " as deleted",
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
    }

    // Restart from here when connection established and DELETE flag set
    public Flux<ApiCallRc> updateSatellitesForResourceDelete(
        ResponseContext contextRef,
        Set<NodeName> nodeNames,
        ResourceName rscName
    )
    {
        return scopeRunner
            .fluxInTransactionlessScope(
                "Update for resource deletion",
                lockGuardFactory.buildDeferred(LockType.READ, LockObj.RSC_DFN_MAP),
                () -> updateSatellitesInScope(contextRef, nodeNames, rscName),
                MDC.getCopyOfContextMap()
            );
    }

    private Flux<ApiCallRc> updateSatellitesInScope(
        ResponseContext contextRef,
        Set<NodeName> nodeNames,
        ResourceName rscName
    )
    {
        ResourceDefinition rscDfn = null;
        for (NodeName nodeName : nodeNames)
        {
            Resource rsc = ctrlApiDataLoader.loadRsc(nodeName, rscName, false);
            if (rsc != null)
            {
                rscDfn = rsc.getResourceDefinition();
                break;
            }
        }

        Flux<ApiCallRc> flux;
        if (rscDfn != null)
        {
            Flux<ApiCallRc> nextStep = deleteData(contextRef, nodeNames, rscName);
            flux = ctrlSatelliteUpdateCaller.updateSatellites(rscDfn, nextStep)
                .transform(updateResponses -> CtrlResponseUtils.combineResponses(
                    errorReporter,
                    updateResponses,
                    rscName,
                    nodeNames,
                    "Cleaning up {1} on {0}",
                    "Notified {0} that {1} is being cleaned up on Node(s): '" + nodeNames + "'"
                )
                )
                .concatWith(nextStep)
                .onErrorResume(CtrlResponseUtils.DelayedApiRcException.class, ignored -> Flux.empty());
        }
        else
        {
            flux = Flux.empty();
        }
        return flux;
    }

    public Flux<ApiCallRc> deleteData(ResponseContext contextRef, Set<NodeName> nodeNames, ResourceName rscName)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Delete resource data",
                lockGuardFactory.buildDeferred(LockType.WRITE, LockObj.RSC_DFN_MAP),
                () -> deleteDataInTransaction(contextRef, nodeNames, rscName)
            );
    }

    private Flux<ApiCallRc> deleteDataInTransaction(
        ResponseContext contextRef,
        Set<NodeName> nodeNames,
        ResourceName rscName
    )
    {
        List<Resource> rscList = new ArrayList<>();
        for (NodeName nodeName : nodeNames)
        {
            Resource rsc = ctrlApiDataLoader.loadRsc(nodeName, rscName, false);
            if (rsc != null)
            {
                rscList.add(rsc);
            }
        }

        Flux<ApiCallRc> flux;

        if (rscList.isEmpty())
        {
            flux = Flux.empty();
        }
        else
        {
            ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
            Set<ResourceDefinition> rscDfnsToCheck = new HashSet<>();
            for (Resource rsc : rscList)
            {
                UUID rscUuid = rsc.getUuid();
                String descriptionFirstLetterCaps = firstLetterCaps(getRscDescription(rsc));
                ResourceDefinition rscDfn = rsc.getResourceDefinition();

                cleanupAndDelete(rsc);

                if (rscDfn.getResourceCount() == 0)
                {
                    // remove primary flag
                    errorReporter.logDebug(
                        String.format("Resource definition '%s' empty, deleting primary flag.", rscName)
                    );
                    removePropPrimarySetPrivileged(rscDfn);
                }
                rscDfnsToCheck.add(rscDfn);

                apiCallRc.addEntry(
                    ApiCallRcImpl
                        .entryBuilder(ApiConsts.DELETED, descriptionFirstLetterCaps + " deletion complete.")
                        .setDetails(descriptionFirstLetterCaps + " UUID was: " + rscUuid)
                        .build()
                );
            }

            Flux<ApiCallRc> autoFlux = Flux.empty();
            for (ResourceDefinition rscDfn : rscDfnsToCheck)
            {
                // check auto-helpers if we need a tiebreaker or something since we just deleted a resource
                autoFlux = autoFlux.concatWith(
                    rscAutoHelperProvider.get()
                        .manage(
                            new AutoHelperContext(apiCallRc, contextRef, rscDfn)
                        )
                        .getFlux()
                );
            }

            ctrlTransactionHelper.commit();

            flux = Flux.<ApiCallRc>just(apiCallRc)
                .concatWith(autoFlux);
        }

        return flux;
    }

    public ApiCallRc ensureNotInUse(Resource rsc)
    {
        return ensureNotInUse(rsc, true);
    }

    public ApiCallRc ensureNotInUse(Resource rsc, boolean throwApiExc)
    {
        ApiCallRcImpl resp = new ApiCallRcImpl();
        ResourceName rscName = rsc.getResourceDefinition().getName();
        NodeName nodeName = rsc.getNode().getName();

        Boolean inUse;
        Peer peer = getPeerPrivileged(rsc.getNode());
        try (LockGuard ignored = LockGuard.createLocked(peer.getSatelliteStateLock().readLock()))
        {
            inUse = peer.getSatelliteState().getFromResource(
                rscName, SatelliteResourceState::isInUse);
        }

        if (inUse != null && inUse)
        {
            ApiCallRcImpl.ApiCallRcEntry err = ApiCallRcImpl
                .entryBuilder(
                    ApiConsts.FAIL_IN_USE,
                    String.format("Resource '%s' is still in use.", rscName)
                )
                .setCause("Resource is mounted/in use.")
                .setCorrection(String.format("Un-mount resource '%s' on the node '%s'.", rscName, nodeName))
                .build();
            resp.addEntry(err);
            if (throwApiExc)
            {
                throw new ApiRcException(err);
            }
        }

        return resp;
    }

    public ApiCallRc ensureNotLastDisk(Resource rsc)
    {
        return ensureNotLastDisk(rsc, true);
    }

    public ApiCallRc ensureNotLastDisk(Resource rsc, boolean throwApiExc)
    {
        ApiCallRcImpl resp = new ApiCallRcImpl();
        try
        {
            AccessContext accCtx = peerAccCtx.get();
            boolean isDiskless = rsc.isDrbdDiskless(accCtx) || rsc.isNvmeInitiator(accCtx) ||
                rsc.isEbsInitiator(accCtx);
            if (
                !isDiskless &&
                    rsc.getResourceDefinition().hasDisklessNotDeleting(accCtx) &&
                    rsc.getResourceDefinition().getNotDeletedDiskfulCount(accCtx) == 1
            )
            {
                ApiCallRcImpl.ApiCallRcEntry err = ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.FAIL_IN_USE,
                        String.format(
                            "Last resource of '%s' with disk still has diskless resources attached.",
                            rsc.getResourceDefinition().getName())
                    )
                    .setCause("Resource still has diskless users.")
                    .setCorrection("Before deleting this resource, delete the diskless resources attached to it.")
                    .build();

                resp.addEntry(err);
                if (throwApiExc)
                {
                    throw new ApiRcException(err);
                }
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            resp.addEntry(ApiCallRcImpl.copyFromLinstorExc(ApiConsts.FAIL_ACC_DENIED_RSC, accDeniedExc));
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "check whether is last with disk " + getRscDescriptionInline(rsc),
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        return resp;
    }

    private Peer getPeerPrivileged(Node node)
    {
        Peer nodePeer;
        try
        {
            nodePeer = node.getPeer(apiCtx);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return nodePeer;
    }

    public void cleanupAndDelete(Resource rsc)
    {
        retryRscTask.remove(rsc);
        deletePrivileged(rsc);
    }

    private void deletePrivileged(Resource rsc)
    {
        try
        {
            rsc.delete(apiCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
    }

    private void removePropPrimarySetPrivileged(ResourceDefinition rscDfn)
    {
        try
        {
            rscDfn.getProps(apiCtx).removeProp(InternalApiConsts.PROP_PRIMARY_SET);
        }
        catch (InvalidKeyException | AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
    }
}
