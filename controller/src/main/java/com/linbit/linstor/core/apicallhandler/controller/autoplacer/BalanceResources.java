package com.linbit.linstor.core.apicallhandler.controller.autoplacer;

import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscAutoPlaceApiCallHandler;
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
import com.linbit.linstor.satellitestate.SatelliteState;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.locks.LockGuard;
import com.linbit.locks.LockGuardFactory;
import com.linbit.utils.Pair;
import com.linbit.utils.StringUtils;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import reactor.core.publisher.Flux;
import reactor.util.context.Context;

import static com.linbit.locks.LockGuardFactory.LockObj.RSC_DFN_MAP;
import static com.linbit.locks.LockGuardFactory.LockType.WRITE;

@Singleton
public class BalanceResources
{
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final ErrorReporter log;
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
    private static final int DFLT_SKIP_DISK_LIMIT = 1;

    @Inject
    public BalanceResources(
        ErrorReporter errorReporterRef,
        SystemConfRepository systemConfRepositoryRef,
        ResourceDefinitionRepository rscDfnRepoRef,
        AutoUnplacer autoUnplacerRef,
        ResourceStateEvent resourceStateEventRef,
        EventWaiter eventWaiterRef,
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlRscDeleteApiCallHandler rscDeleteApiCallHandlerRef,
        CtrlRscAutoPlaceApiCallHandler ctrlRscAutoPlaceApiCallHandlerRef

    )
    {
        log = errorReporterRef;
        systemConfRepository = systemConfRepositoryRef;
        rscDfnRepo = rscDfnRepoRef;
        autoUnplacer = autoUnplacerRef;
        resourceStateEvent = resourceStateEventRef;
        eventWaiter = eventWaiterRef;
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        rscDeleteApiCallHandler = rscDeleteApiCallHandlerRef;
        ctrlRscAutoPlaceApiCallHandler = ctrlRscAutoPlaceApiCallHandlerRef;
    }

    private boolean hasAtLeastOneUpToDate(ResourceDefinition rscDfn)
    {
        boolean result = false;
        List<Resource> rscs = rscDfn.streamResource().collect(Collectors.toList());
        for (var rsc : rscs)
        {
            if (!rsc.isDiskless() &&
                rsc.getStateFlags().isUnset(Resource.Flags.DELETE, Resource.Flags.DRBD_DELETE))
            {
                SatelliteResourceState stltRscState = rsc.getNode().getPeer()
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

    private long getGracePeriod()
    {
        long gracePeriod = DEFAULT_GRACE_PERIOD_SECS;
        String gracePeriodStr = systemConfRepository.getCtrlConfForView()
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

    /**
     * <p>Checks the <b>creation</b> date of the resource against the {@link #getGracePeriod()}.</p>
     *
     * <p>NOTE: A resource that was just toggled, or restored from an evicted state or got its DRBD metadata just
     * recreated is still considered old, since these operations do not reset the creation date.</p>
     */
    private boolean isResourceInGracePeriod(Resource rsc)
    {
        long gracePeriodSecs = getGracePeriod();
        long nowSecs = System.currentTimeMillis() / 1000L;
        final Instant deadLine = Instant.ofEpochMilli((nowSecs - gracePeriodSecs) * 1000L);
        final Instant beforeDeadLine = Instant.ofEpochMilli((nowSecs - gracePeriodSecs - 10) * 1000L);

        return rsc.getCreateTimestamp().orElse(beforeDeadLine).isAfter(deadLine) ||
            rsc.getCreateTimestamp().orElse(
                Instant.ofEpochMilli(AbsResource.CREATE_DATE_INIT_VALUE))
                .toEpochMilli() == AbsResource.CREATE_DATE_INIT_VALUE;
    }

    /**
     * Get a list of resources that shouldn't be removed from the resource definition
     * (diskless, inuse, grace period, ...)
     * @param resources to evaluate
     * @return A list of resources that should stay in place.
     */
    private List<Resource> getFixedResources(List<Resource> resources)
    {
        var fixed = new ArrayList<Resource>();
        for (var rsc : resources)
        {
            // do not delete:
            // * diskless resources
            // * resource created within the grace period
            // * resources that don't have a creation date yet (or very old resources before creation date was added)
            if (rsc.isDiskless() || isResourceInGracePeriod(rsc))
            {
                fixed.add(rsc);
            }
            else
            {
                Peer peer = rsc.getNode().getPeer();
                @Nullable SatelliteState stltState = peer.getSatelliteState();
                if (stltState != null)
                {
                    @Nullable SatelliteResourceState stltRscState = stltState
                        .getResourceStates()
                        .get(rsc.getResourceDefinition().getName());

                    if (stltRscState == null || Boolean.TRUE.equals(stltRscState.isInUseOrOpen()))
                    {
                        fixed.add(rsc);
                    }
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
        throws InvalidKeyException
    {
        return rscDeleteApiCallHandler.deleteResource(
            rsc.getNode().getName().displayValue,
            rsc.getResourceDefinition().getName().displayValue
        ).contextWrite(
            Context.of(
                ApiModule.API_CALL_NAME,
                "Deleting excess " + CtrlRscApiCallHandler.getRscDescription(rsc),
                Peer.class,
                rsc.getNode().getPeer()
            )
        );
    }

    @SuppressWarnings("checkstyle:returncount")
    private boolean shouldIgnoreRscDfn(ResourceDefinition rscDfn)
    {
        @Nullable String rscIgnoreReason = findRscIgnoreReason(rscDfn);

        if (rscIgnoreReason != null)
        {
            log.logDebug("BalanceResourcesTask/%s: %s", rscDfn.getName(), rscIgnoreReason);
            return true;
        }

        if (!rscDfn.usesLayer(DeviceLayerKind.DRBD))
        {
            log.logDebug("BalanceResourcesTask/%s: Ignore because no DRBD", rscDfn.getName());
            return true;
        }

        if (rscDfn.getFlags().isSet(ResourceDefinition.Flags.DELETE))
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

        if (hasUnexpectedlyNotUpToDateDiskful(rscDfn))
        { // this should not trigger in case of SkipDisk
            log.logDebug(
                "BalanceResourcesTask/%s: Ignore because a non-skip-disk diskful resource is not UpToDate",
                rscDfn.getName()
            );
            return true;
        }

        { // check SkipDiskLimit
            PriorityProps prioProps = new PriorityProps(
                rscDfn.getProps(),
                rscDfn.getResourceGroup().getProps(),
                systemConfRepository.getCtrlConfForView()
            );
            @Nullable String skipDiskLimitStr = prioProps.getProp(ApiConsts.KEY_BALANCE_RESOURCES_SKIP_DISK_LIMIT);
            int skipDiskLimit;
            try
            {
                skipDiskLimit = skipDiskLimitStr == null ? DFLT_SKIP_DISK_LIMIT : Integer.parseInt(skipDiskLimitStr);
            }
            catch (NumberFormatException nfe)
            {
                log.logWarning(
                    "BalanceResources/%s: Failed to parse '%s'. Defaulting to %d as SkipDiskLimit",
                    rscDfn.getName(),
                    skipDiskLimitStr,
                    DFLT_SKIP_DISK_LIMIT
                );
                skipDiskLimit = DFLT_SKIP_DISK_LIMIT;
            }
            List<Resource> skipDiskResources = getResourcesWithSkipDisk(rscDfn.streamResource().toList());
            if (skipDiskResources.size() > skipDiskLimit)
            {
                log.logDebug(
                    "BalanceResourcesTask/%s: Ignore because rscDfn has already %d resources with SkipDisk, " +
                        "max %d tolerated",
                    rscDfn.getName(),
                    skipDiskResources.size(),
                    skipDiskLimit
                );
                return true;
            }
        }

        return false;
    }

    private @Nullable String findRscIgnoreReason(ResourceDefinition rscDfnRef)
    {
        @Nullable String ret = null;
        for (Resource rsc : rscDfnRef.streamResource().toList())
        {
            if (isResourceInGracePeriod(rsc))
            {
                ret = "Ignore because of grace period";
            }
            else
            {
                // TODO we do not want to touch a resource definition where at least one resource is currently
                // in a transition-state. this check should eventually be replaced with a "has the RD been
                // modified/touched by the user in the last 1h", once we implement the necessary tracking
                // for that.
                boolean isInTransitionalState = rsc.getStateFlags()
                    .isSomeSet(
                        Resource.Flags.DELETE,
                        Resource.Flags.DRBD_DELETE,
                        Resource.Flags.DISK_ADD_REQUESTED,
                        Resource.Flags.DISK_ADDING,
                        Resource.Flags.DISK_REMOVE_REQUESTED,
                        Resource.Flags.DISK_REMOVING,
                        Resource.Flags.EVACUATE,
                        Resource.Flags.INACTIVATING,
                        Resource.Flags.REACTIVATE,
                        Resource.Flags.RESTORE_FROM_SNAPSHOT
                    );
                if (isInTransitionalState)
                {
                    ret = String.format(
                        "Ignore because resource on node %s has transitional flags set: %s",
                        rsc.getNode().getName().displayValue,
                        FlagsHelper.toStringList(Resource.Flags.class, rsc.getStateFlags().getFlagsBits())
                    );
                }
            }
            if (ret != null)
            {
                break;
            }
        }
        return ret;
    }

    /**
     * Checks if the given resource definition is disabled to balance
     * @param rscDfn resource definition to check
     * @return true if the resource definition shouldn't be balanced (because of prop disable)
     */
    private boolean isRscDfnDisabled(ResourceDefinition rscDfn)
    {
        ResourceGroup rscGrp = rscDfn.getResourceGroup();
        var prioProps = new PriorityProps(
            rscDfn.getProps(),
            rscGrp.getProps(),
            systemConfRepository.getCtrlConfForView());

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
     */
    @SuppressWarnings({"checkstyle:DescendantToken", "checkstyle:returnCount"})
    private boolean areAllVolumesInGoodReplicationState(ResourceDefinition rscDfn)
    {
        for (Resource rsc : rscDfn.streamResource().collect(Collectors.toList()))
        {
            var rscStates = rsc.getNode().getPeer().getSatelliteState().getResourceStates();
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
     * A resource the model considers diskful, that is NOT a skip-disk resource, but whose disk is not
     * UpToDate (e.g. an in-progress or a failed toggle-disk that never attached, reporting Diskless).
     * Such a resource inflates the diskful count and must not drive removals until it recovers or is
     * cleaned up.
     * Skip-disk resources are intentionally excluded so the existing skip-disk handling is unaffected.
     */
    private boolean hasUnexpectedlyNotUpToDateDiskful(ResourceDefinition rscDfn)
    {
        boolean ret = false;
        List<Resource> diskful = rscDfn.getNotDeletedDiskful(); // flag-diskful, excludes DRBD_DISKLESS
        diskful.removeAll(getResourcesWithSkipDisk(diskful)); // do not test SkipDisk rscs
        for (Resource rsc : diskful)
        {
            Peer peer = rsc.getNode().getPeer();
            if (peer.getSatelliteState() != null)
            {
                @Nullable SatelliteResourceState rs = peer.getSatelliteState()
                    .getResourceStates()
                    .get(rscDfn.getName());
                if (rs != null && !rs.allVolumesUpToDate())
                {
                    ret = true;
                    break;
                }
            }
        }
        return ret;
    }

    /**
     * Filter resources that should really considered as UpToDate diskfull resources.
     *
     * @param resources Resource to filter
     */
    private void filterDiskfull(List<Resource> resources)
    {
        List<Resource> skipDiskResources = getResourcesWithSkipDisk(resources);
        resources.removeAll(skipDiskResources);
    }

    private List<Resource> getResourcesWithSkipDisk(List<Resource> resources)
    {
        List<Resource> toRemove = new ArrayList<>();
        for (var res : resources)
        {
            @Nullable String skipDiskProp = res.getProps()
                .getProp(
                ApiConsts.KEY_DRBD_SKIP_DISK, ApiConsts.NAMESPC_DRBD_OPTIONS);
            if (StringUtils.propTrueOrYes(skipDiskProp))
            {
                toRemove.add(res);
            }
        }
        return toRemove;
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
            for (var rscDfn : rscDfnRepo.getMapForView().values())
            {
                if (shouldIgnoreRscDfn(rscDfn))
                {
                    // only work on DRBD resources, not in deleting, with atleast one resource
                    continue;
                }

                int replicaCount = rscDfn.getResourceGroup().getAutoPlaceConfig().getReplicaCount();
                List<Resource> notDeletedDiskful = rscDfn.getNotDeletedDiskful();
                filterDiskfull(notDeletedDiskful);
                int notDeletedDiskfulCount = notDeletedDiskful.size();
                if (notDeletedDiskfulCount < replicaCount)
                {
                    log.logInfo("BalanceResourcesTask/%s needs more diskful", rscDfn.getName());
                    adjustRscDfns.add(rscDfn);
                }
                else if (notDeletedDiskfulCount > replicaCount)
                {
                    var fixedResources = getFixedResources(rscDfn.streamResource().collect(Collectors.toList()));

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

        // adjust rsc dfns to meet rscgrp replica count
        for (var rscDfn : adjustRscDfns)
        {
            flux = flux.concatWith(
                ctrlRscAutoPlaceApiCallHandler.autoPlace(
                    rscDfn.getName().displayValue,
                    rscDfn.getResourceGroup().getAutoPlaceConfig().getApiData(),
                    false,
                    Collections.emptyList()
                )
            );
        }

        flux
            .contextWrite(
                Context.of(
                    ApiModule.API_CALL_NAME,
                    "Balance resources Adjust and delete",
                    Peer.class,
                    new PeerTask("BalanceResourceTask"))
            )
            .timeout(Duration.ofSeconds(timeoutSecs))
            .doOnTerminate(() -> isRunning.set(false))
            .subscribe(ignoredResults -> { }, log::reportError);

        return new Pair<>(adjustRscDfns.size(), deletedRscCount);
    }
}
