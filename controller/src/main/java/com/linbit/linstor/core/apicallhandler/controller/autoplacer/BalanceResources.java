package com.linbit.linstor.core.apicallhandler.controller.autoplacer;

import com.linbit.ImplementationError;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscAutoPlaceApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscCrtApiHelper;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscDeleteApiCallHandler;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.repository.ResourceDefinitionRepository;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.event.EventWaiter;
import com.linbit.linstor.event.ObjectIdentifier;
import com.linbit.linstor.event.common.ResourceStateEvent;
import com.linbit.linstor.layer.drbd.drbdstate.ReplState;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.netcom.PeerNotConnectedException;
import com.linbit.linstor.netcom.PeerTask;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.satellitestate.SatelliteResourceState;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.locks.LockGuard;
import com.linbit.locks.LockGuardFactory;
import com.linbit.utils.Pair;
import com.linbit.utils.StringUtils;

import static com.linbit.locks.LockGuardFactory.LockObj.RSC_DFN_MAP;
import static com.linbit.locks.LockGuardFactory.LockType.WRITE;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import reactor.core.publisher.Flux;
import reactor.util.context.Context;

@Singleton
public class BalanceResources
{
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final AccessContext sysCtx;
    private final ErrorReporter log;
    private final Autoplacer autoplacer;
    private final AutoUnplacer autoUnplacer;
    private final SystemConfRepository systemConfRepository;
    private final ResourceDefinitionRepository rscDfnRepo;
    private final ResourceStateEvent resourceStateEvent;
    private final EventWaiter eventWaiter;
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlRscDeleteApiCallHandler rscDeleteApiCallHandler;
    private final CtrlRscAutoPlaceApiCallHandler ctrlRscAutoPlaceApiCallHandler;

    private static final long DEFAULT_GRACE_PERIOD_SECS = 3600;

    @Inject
    public BalanceResources(
        @SystemContext AccessContext sysCtxRef,
        ErrorReporter errorReporterRef,
        SystemConfRepository systemConfRepositoryRef,
        ResourceDefinitionRepository rscDfnRepoRef,
        Autoplacer autoplacerRef,
        AutoUnplacer autoUnplacerRef,
        ResourceStateEvent resourceStateEventRef,
        EventWaiter eventWaiterRef,
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlRscDeleteApiCallHandler rscDeleteApiCallHandlerRef,
        CtrlRscAutoPlaceApiCallHandler ctrlRscAutoPlaceApiCallHandlerRef

    )
    {
        sysCtx = sysCtxRef;
        log = errorReporterRef;
        systemConfRepository = systemConfRepositoryRef;
        rscDfnRepo = rscDfnRepoRef;
        autoplacer = autoplacerRef;
        autoUnplacer = autoUnplacerRef;
        resourceStateEvent = resourceStateEventRef;
        eventWaiter = eventWaiterRef;
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        rscDeleteApiCallHandler = rscDeleteApiCallHandlerRef;
        ctrlRscAutoPlaceApiCallHandler = ctrlRscAutoPlaceApiCallHandlerRef;
    }

    private boolean hasAtLeastOneUpToDate(ResourceDefinition rscDfn) throws AccessDeniedException
    {
        boolean result = false;
        List<Resource> rscs = rscDfn.streamResource(sysCtx).collect(Collectors.toList());
        for (var rsc : rscs)
        {
            if (!rsc.isDiskless(sysCtx) &&
                rsc.getStateFlags().isUnset(sysCtx, Resource.Flags.DELETE, Resource.Flags.DRBD_DELETE))
            {
                SatelliteResourceState stltRscState = rsc.getNode().getPeer(sysCtx)
                    .getSatelliteState()
                    .getResourceStates()
                    .get(rsc.getResourceDefinition().getName());
                if (stltRscState != null && stltRscState.allVolumesUpToDate())
                {
                    result = true;
                    break;
                }
            }
        }
        return result;
    }

    private long getGracePeriod() throws AccessDeniedException
    {
        long gracePeriod = DEFAULT_GRACE_PERIOD_SECS;
        String gracePeriodStr = systemConfRepository.getCtrlConfForView(sysCtx)
            .getProp(ApiConsts.KEY_BALANCE_RESOURCES_GRACE_PERIOD);
        try
        {
            if (gracePeriodStr != null)
            {
                gracePeriod = Long.parseLong(gracePeriodStr);
            }
        }
        catch (NumberFormatException nfe)
        {
            log.reportError(nfe);
        }
        return gracePeriod;
    }

    private boolean isResourceInGracePeriod(Resource rsc) throws AccessDeniedException
    {
        long gracePeriodSecs = getGracePeriod();
        long nowSecs = System.currentTimeMillis() / 1000L;
        final Date deadLine = new Date((nowSecs - gracePeriodSecs) * 1000L);
        final Date beforeDeadLine = new Date((nowSecs - gracePeriodSecs - 10) * 1000L);

        return rsc.getCreateTimestamp().orElse(beforeDeadLine).after(deadLine) ||
            rsc.getCreateTimestamp().orElse(
                new Date(AbsResource.CREATE_DATE_INIT_VALUE)).getTime() == AbsResource.CREATE_DATE_INIT_VALUE;
    }

    /**
     * Get a list of resources that shouldn't be removed from the resource definition
     * (diskless, inuse, grace period, ...)
     * @param resources to evaluate
     * @return A list of resources that should stay in place.
     * @throws AccessDeniedException shouldn't happen as we use the sysCtx
     */
    private List<Resource> getFixedResources(
        List<Resource> resources, Map<Resource, Integer> onlineNodeIds) throws AccessDeniedException
    {
        var fixed = new ArrayList<Resource>();
        for (var rsc : resources)
        {
            var onlineNodeIdsNotSelf = new HashMap<>(onlineNodeIds);
            onlineNodeIdsNotSelf.remove(rsc);
            // do not delete:
            // * diskless resources
            // * resource created within the grace period
            // * resources that don't have a creation date yet (or very old resources before creation date was added)
            if (rsc.isDiskless(sysCtx) || isResourceInGracePeriod(rsc))
            {
                fixed.add(rsc);
            }
            else
            {
                SatelliteResourceState stltRscState = rsc.getNode().getPeer(sysCtx)
                    .getSatelliteState()
                    .getResourceStates()
                    .get(rsc.getResourceDefinition().getName());

                if (stltRscState == null ||
                    Boolean.TRUE.equals(stltRscState.isInUse()) ||
                    !stltRscState.allVolumesUpToDate() ||
                    !stltRscState.isReady(onlineNodeIdsNotSelf.values()))
                {
                    fixed.add(rsc);
                }
            }
        }
        return fixed;
    }

    private Flux<ApiCallRc> removeExcessFlux(Resource rsc)
    {
        return eventWaiter.waitForStream(
                resourceStateEvent.get(),
                ObjectIdentifier.resource(rsc.getNode().getName(), rsc.getResourceDefinition().getName())
            )
            .skipUntil(usage -> usage.getUpToDate())
            .next()
            .thenMany(
                scopeRunner.fluxInTransactionalScope(
                    "Delete excess after BalanceResourceTask of " + CtrlRscApiCallHandler.getRscDescription(rsc),
                    lockGuardFactory.buildDeferred(WRITE, LockGuardFactory.LockObj.RSC_DFN_MAP),
                    () -> removeExcessFluxInTransaction(rsc)
                )
            )
            .onErrorResume(PeerNotConnectedException.class, ignored -> Flux.empty());
    }

    private Flux<ApiCallRc> removeExcessFluxInTransaction(Resource rsc)
        throws InvalidKeyException, AccessDeniedException
    {
        return rscDeleteApiCallHandler.deleteResource(
            rsc.getNode().getName().displayValue,
            rsc.getResourceDefinition().getName().displayValue
        ).contextWrite(
            Context.of(
                ApiModule.API_CALL_NAME,
                "Deleting excess " + CtrlRscApiCallHandler.getRscDescription(rsc),
                AccessContext.class,
                sysCtx,
                Peer.class,
                rsc.getNode().getPeer(sysCtx)
            )
        );
    }

    private boolean shouldIgnoreRscDfn(ResourceDefinition rscDfn) throws AccessDeniedException
    {
        boolean someRscIgnored = false;
        for (var rsc : rscDfn.streamResource(sysCtx).collect(Collectors.toList()))
        {
            if (isResourceInGracePeriod(rsc))
            {
                someRscIgnored = true;
                break;
            }
        }

        if (someRscIgnored)
        {
            log.logDebug("BalanceResourcesTask/%s: Ignore because of grace period", rscDfn.getName());
            return true;
        }

        if (!rscDfn.usesLayer(sysCtx, DeviceLayerKind.DRBD))
        {
            log.logDebug("BalanceResourcesTask/%s: Ignore because no DRBD", rscDfn.getName());
            return true;
        }

        if (rscDfn.getFlags().isSet(sysCtx, ResourceDefinition.Flags.DELETE))
        {
            log.logDebug("BalanceResourcesTask/%s: Ignore because rscDfn in delete", rscDfn.getName());
            return true;
        }

        if (isRscDfnDisabled(rscDfn))
        {
            log.logDebug("BalanceResourcesTask/%s: Ignore because rscDfn disabled by prop", rscDfn.getName());
            return true;
        }

        if (!hasAtLeastOneUpToDate(rscDfn))
        {
            log.logDebug("BalanceResourcesTask/%s: Ignore because no UpToDate resource", rscDfn.getName());
            return true;
        }

        if (!areAllVolumesInGoodReplicationState(rscDfn))
        {
            log.logDebug(
                "BalanceResourcesTask/%s: Ignore because some volumes do not have an established replication",
                rscDfn.getName()
            );
            return true;
        }

        return false;
    }

    /**
     * Checks if the given resource definition is disabled to balance
     * @param rscDfn resource definition to check
     * @return true if the resource definition shouldn't be balanced (because of prop disable)
     * @throws AccessDeniedException if reading props failed
     */
    private boolean isRscDfnDisabled(ResourceDefinition rscDfn) throws AccessDeniedException
    {
        ResourceGroup rscGrp = rscDfn.getResourceGroup();
        var prioProps = new PriorityProps(
            rscDfn.getProps(sysCtx),
            rscGrp.getProps(sysCtx),
            systemConfRepository.getCtrlConfForView(sysCtx));

        return "false".equalsIgnoreCase(prioProps.getProp(ApiConsts.KEY_BALANCE_RESOURCES_ENABLED, null, "true"));
    }

    @SuppressWarnings({"checkstyle:DescendantToken", "checkstyle:returnCount"})
    private boolean anyBadReplicationState(Map<NodeName, ReplState> replStateMap)
    {
        for (var replicationState : replStateMap.values())
        {
            if (!(replicationState == null ||
                ReplState.ESTABLISHED.equals(replicationState) ||
                ReplState.OFF.equals(replicationState)))
            {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks if all volumes of the given resource definition are in a "good" replication state.
     * This is to prevent resources getting removed that are e.g. currently SyncSource.
     * @param rscDfn resource definition to check
     * @return true if all volumes are established, else false.
     * @throws AccessDeniedException should not happen as we use sysCtx
     */
    @SuppressWarnings({"checkstyle:DescendantToken", "checkstyle:returnCount"})
    private boolean areAllVolumesInGoodReplicationState(ResourceDefinition rscDfn) throws AccessDeniedException
    {
        for (Resource rsc : rscDfn.streamResource(sysCtx).collect(Collectors.toList()))
        {
            var rscStates = rsc.getNode().getPeer(sysCtx).getSatelliteState().getResourceStates();
            var satRscStates = rscStates.get(rscDfn.getName());
            if (satRscStates != null)
            {
                for (var volEntry : satRscStates.getVolumeStates().entrySet())
                {
                    if (anyBadReplicationState(volEntry.getValue().getReplicationStateMap()))
                    {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    /**
     * Filter resources that should really considered as UpToDate diskfull resources.
     *
     * @param resources Resource to filter
     * @throws AccessDeniedException should not we use sysctx
     */
    private void filterDiskfull(List<Resource> resources) throws AccessDeniedException
    {
        List<Resource> toRemove = new ArrayList<>();
        for(var res : resources)
        {
            String skipDiskProp = res.getProps(sysCtx).getProp(
                ApiConsts.KEY_DRBD_SKIP_DISK, ApiConsts.NAMESPC_DRBD_OPTIONS);
            if (StringUtils.propTrueOrYes(skipDiskProp))
            {
                toRemove.add(res);
            }
        }
        resources.removeAll(toRemove);
    }

    /**
     * Loops through all resource definitions and tries to fulfill the linked resource groups place counts.
     * @param timeoutSecs Timeout in seconds of the adjust and delete flux
     * @return a Pair with numberAdjusted, deletedResources
     */
    public Pair<Integer, Integer> balanceResources(long timeoutSecs)
    {
        if (isRunning.get())
        {
            log.logWarning("BalanceResources is currently running early exit;");
            return new Pair<>(0, 0);
        }

        isRunning.set(true);
        int deletedRscCount = 0;
        var adjustRscDfns = new ArrayList<ResourceDefinition>();
        Flux<ApiCallRc> flux = Flux.empty();

        try (
            LockGuard ignored = lockGuardFactory.build(WRITE, RSC_DFN_MAP)
        )
        {
            for (var rscDfn : rscDfnRepo.getMapForView(sysCtx).values())
            {
                if (shouldIgnoreRscDfn(rscDfn))
                {
                    // only work on DRBD resources, not in deleting, with atleast one resource
                    continue;
                }

                int replicaCount = rscDfn.getResourceGroup().getAutoPlaceConfig().getReplicaCount(sysCtx);
                List<Resource> notDeletedDiskful = rscDfn.getNotDeletedDiskful(sysCtx);
                filterDiskfull(notDeletedDiskful);
                int notDeletedDiskfulCount = notDeletedDiskful.size();
                if (notDeletedDiskfulCount < replicaCount)
                {
                    log.logInfo("BalanceResourcesTask/%s needs more diskful", rscDfn.getName());
                    adjustRscDfns.add(rscDfn);
                }
                else if (notDeletedDiskfulCount > replicaCount)
                {
                    var onlineNodeIds = CtrlRscCrtApiHelper.getOnlineNodeIds(rscDfn, sysCtx);
                    var fixedResources = getFixedResources(
                        rscDfn.streamResource(sysCtx).collect(Collectors.toList()),
                        onlineNodeIds
                    );

                    // loop through rscDfn resources, until we meet replicaCount or are out of resource to delete
                    Resource toDelete = autoUnplacer.unplace(rscDfn, fixedResources);
                    while (toDelete != null && notDeletedDiskfulCount > replicaCount)
                    {
                        log.logInfo(
                            "BalanceResourcesTask/%s going to delete: %s",
                            rscDfn.getName(),
                            toDelete
                        );
                        flux = flux.concatWith(removeExcessFlux(toDelete));
                        notDeletedDiskfulCount--;
                        deletedRscCount++;
                        fixedResources.add(toDelete);
                        toDelete = autoUnplacer.unplace(rscDfn, fixedResources);
                    }
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            log.reportError(new ImplementationError(exc));
        }

        // adjust rsc dfns to meet rscgrp replica count
        for (var rscDfn : adjustRscDfns)
        {
            flux = flux.concatWith(
                ctrlRscAutoPlaceApiCallHandler.autoPlace(
                    rscDfn.getName().displayValue,
                    rscDfn.getResourceGroup().getAutoPlaceConfig().getApiData()
                )
            );
        }

        flux
            .contextWrite(
                Context.of(
                    ApiModule.API_CALL_NAME,
                    "Balance resources Adjust and delete",
                    AccessContext.class,
                    sysCtx,
                    Peer.class,
                    new PeerTask("BalanceResourceTask", sysCtx))
            )
            .timeout(Duration.ofSeconds(timeoutSecs))
            .doOnTerminate(() -> isRunning.set(false))
            .subscribe(ignoredResults -> { }, log::reportError);

        return new Pair<>(adjustRscDfns.size(), deletedRscCount);
    }
}
