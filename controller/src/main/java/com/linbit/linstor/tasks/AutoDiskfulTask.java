package com.linbit.linstor.tasks;

import com.linbit.ImplementationError;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.api.pojo.AutoSelectFilterPojo;
import com.linbit.linstor.api.pojo.builder.AutoSelectFilterBuilder;
import com.linbit.linstor.core.BackgroundRunner;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscDeleteApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscToggleDiskApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.autoplacer.AutoUnplacer;
import com.linbit.linstor.core.apicallhandler.controller.autoplacer.Autoplacer;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.repository.ResourceDefinitionRepository;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.event.EventWaiter;
import com.linbit.linstor.event.ObjectIdentifier;
import com.linbit.linstor.event.common.ResourceStateEvent;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.netcom.PeerNotConnectedException;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.satellitestate.SatelliteResourceState;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.utils.LayerUtils;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.inject.Key;
import org.slf4j.MDC;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;

@Singleton
public class AutoDiskfulTask implements TaskScheduleService.Task
{
    private static final long MINUTES_TO_MS = 1000 * 60L;

    private static final long TASK_TIMEOUT = 10_000;

    private static final HashSet<String> DO_NOT_CLEANUP_TYPES = new HashSet<>();

    private final AccessContext sysCtx;
    private final ErrorReporter errorReporter;
    private final CtrlRscToggleDiskApiCallHandler toggleDisklHandler;
    private final Autoplacer autoplacer;
    private final AutoUnplacer autoUnplacer;
    private final SystemConfRepository systemConfRepository;
    private final ResourceDefinitionRepository rscDfnRepo;

    private final SortedSet<AutoDiskfulConfig> configSet = new TreeSet<>();
    private final TreeMap<Resource, AutoDiskfulConfig> configSetByRsc = new TreeMap<>();

    private final LinStorScope linstorScope;

    private final BackgroundRunner backgroundRunner;
    private final ResourceStateEvent resourceStateEvent;
    private final EventWaiter eventWaiter;
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlRscDeleteApiCallHandler rscDeleteApiCallHandler;


    static
    {
        DO_NOT_CLEANUP_TYPES.add(Resource.DiskfulBy.USER.getValue());
        DO_NOT_CLEANUP_TYPES.add(Resource.DiskfulBy.AUTO_PLACER.getValue());
    }

    @Inject
    public AutoDiskfulTask(
        @SystemContext AccessContext sysCtxRef,
        ErrorReporter errorReporterRef,
        CtrlRscToggleDiskApiCallHandler toggleDisklHandlerRef,
        SystemConfRepository systemConfRepositoryRef,
        ResourceDefinitionRepository rscDfnRepoRef,
        Autoplacer autoplacerRef,
        AutoUnplacer autoUnplacerRef,
        LinStorScope linstorScopeRef,
        BackgroundRunner backgroundRunnerRef,
        ResourceStateEvent resourceStateEventRef,
        EventWaiter eventWaiterRef,
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlRscDeleteApiCallHandler rscDeleteApiCallHandlerRef
    )
    {
        sysCtx = sysCtxRef;
        errorReporter = errorReporterRef;
        toggleDisklHandler = toggleDisklHandlerRef;
        systemConfRepository = systemConfRepositoryRef;
        rscDfnRepo = rscDfnRepoRef;
        autoplacer = autoplacerRef;
        autoUnplacer = autoUnplacerRef;
        linstorScope = linstorScopeRef;
        backgroundRunner = backgroundRunnerRef;
        resourceStateEvent = resourceStateEventRef;
        eventWaiter = eventWaiterRef;
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        rscDeleteApiCallHandler = rscDeleteApiCallHandlerRef;
    }

    /**
     * Checks the {@link PriorityProps} of {@link Resource}, {@link Node}, {@link ResourceDefinition},
     * {@link ResourceGroup} and Controller (in this order) whether or not the
     * {@link ApiConsts#NAMESPC_DRBD_OPTIONS}/{@link ApiConsts#KEY_DRBD_AUTO_DISKFUL} is set.
     *
     * If the property is set, a new autoDiskfull entry is created or the existing updated.
     * If the property is not set, the existing entry will be deleted.
     *
     * @param rsc
     */
    public void update(Resource rsc)
    {
        try
        {
            if (LayerUtils.hasLayer(rsc.getLayerData(sysCtx), DeviceLayerKind.DRBD))
            {
                if (rsc.getStateFlags().isSet(sysCtx, Resource.Flags.DRBD_DISKLESS))
                {
                    String autoDiskful = getPrioProps(rsc)
                        .getProp(ApiConsts.KEY_DRBD_AUTO_DISKFUL, ApiConsts.NAMESPC_DRBD_OPTIONS);
                    SatelliteResourceState rscStates = rsc.getNode().getPeer(sysCtx)
                            .getSatelliteState().getResourceStates().get(rsc.getResourceDefinition().getName());
                    Boolean isPrimary = rscStates != null ? rscStates.isInUse() : null;

                    boolean enableAutoDiskful = autoDiskful != null && isPrimary != null && isPrimary;
                    synchronized (configSet)
                    {
                        AutoDiskfulConfig cfg = configSetByRsc.get(rsc);
                        if (!enableAutoDiskful)
                        {
                            if (cfg != null)
                            {
                                configSet.remove(cfg);
                                configSetByRsc.remove(rsc);
                                errorReporter.logTrace(
                                    "Removed %s to autoDiskfulTask",
                                    CtrlRscApiCallHandler.getRscDescription(rsc)
                                );
                            }
                        }
                        else
                        {
                            // property is in minutes
                            long toggleDiskAfter = Long.parseLong(autoDiskful) * MINUTES_TO_MS;
                            if (cfg != null)
                            {
                                /*
                                 * changing toggleDiskAfter might change the configSet's order.
                                 * To force the TreeSet to reorder, we need to remove and re-add the object
                                 */
                                configSet.remove(cfg);
                                cfg.toggleDiskAfter = toggleDiskAfter;
                                configSet.add(cfg);
                                // no need to update configSetByRsc, as resource did not change
                                errorReporter.logTrace(
                                    "Updated %s to autoDiskfulTask in %dms",
                                    CtrlRscApiCallHandler.getRscDescription(rsc),
                                    (cfg.disklessPrimarySince + toggleDiskAfter) - System.currentTimeMillis()
                                );
                            }
                            else
                            {
                                cfg = new AutoDiskfulConfig(System.currentTimeMillis(), rsc, toggleDiskAfter);
                                configSet.add(cfg);
                                configSetByRsc.put(rsc, cfg);
                                errorReporter.logTrace(
                                    "Added %s to autoDiskfulTask in %dms",
                                    CtrlRscApiCallHandler.getRscDescription(rsc),
                                    toggleDiskAfter
                                );
                            }
                        }
                    }
                }
                else
                {
                    // make sure to delete the no-longer diskless resource if we had it in our sets
                    synchronized (configSet)
                    {
                        AutoDiskfulConfig cfg = configSetByRsc.remove(rsc);
                        if (cfg != null)
                        {
                            configSet.remove(cfg);
                        }
                    }
                }
            }
            else
            {
                errorReporter.logTrace(
                    "Ignoring %s for autoDiskfulTask as it does not contain DRBD layer",
                    CtrlRscApiCallHandler.getRscDescription(rsc)
                );
            }
        }
        catch (AccessDeniedException exc)
        {
            errorReporter.reportError(new ImplementationError(exc));
        }
    }

    private PriorityProps getPrioProps(Resource rsc) throws AccessDeniedException
    {
        return new PriorityProps(
            rsc.getProps(sysCtx),
            rsc.getNode().getProps(sysCtx),
            rsc.getResourceDefinition().getProps(sysCtx),
            rsc.getResourceDefinition().getResourceGroup().getProps(sysCtx),
            // TODO: we should really either unify these props or start working on the generated version...
            systemConfRepository.getStltConfForView(sysCtx),
            systemConfRepository.getCtrlConfForView(sysCtx)
        );
    }

    public void update(Node node)
    {
        try
        {
            node.streamResources(sysCtx)
                .forEach(this::update);
        }
        catch (AccessDeniedException exc)
        {
            errorReporter.reportError(new ImplementationError(exc));
        }
    }

    public void update(ResourceDefinition rscDfn)
    {
        try
        {
            rscDfn.streamResource(sysCtx)
                .forEach(this::update);
        }
        catch (AccessDeniedException exc)
        {
            errorReporter.reportError(new ImplementationError(exc));
        }
    }

    public void update(ResourceGroup rscGrp)
    {
        try
        {
            rscGrp.getRscDfns(sysCtx).stream().flatMap(this::streamRscsPrivileged)
                .forEach(this::update);
        }
        catch (AccessDeniedException exc)
        {
            errorReporter.reportError(new ImplementationError(exc));
        }
    }

    /**
     * Updates ALL resources (controller-level update)
     */
    public void update()
    {
        try
        {
            rscDfnRepo.getMapForView(sysCtx).values().stream().flatMap(this::streamRscsPrivileged)
                .forEach(this::update);
        }
        catch (AccessDeniedException exc)
        {
            errorReporter.reportError(new ImplementationError(exc));
        }
    }

    private Stream<? extends Resource> streamRscsPrivileged(ResourceDefinition rscDfn)
    {
        try
        {
            return rscDfn.streamResource(sysCtx);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    @Override
    public long run(long scheduleAt)
    {
        long nextRunIn = TASK_TIMEOUT;
        ArrayList<AutoDiskfulConfig> cfgsToExecute = new ArrayList<>();
        synchronized (configSet)
        {
            long now = System.currentTimeMillis();
            Iterator<AutoDiskfulConfig> cfgIt = configSet.iterator();
            while (cfgIt.hasNext())
            {
                AutoDiskfulConfig cfg = cfgIt.next();
                long toggleDiskAt = cfg.disklessPrimarySince + cfg.toggleDiskAfter;
                if (toggleDiskAt <= now)
                {
                    cfgsToExecute.add(cfg);
                    configSetByRsc.remove(cfg.rsc);
                    cfgIt.remove();
                }
                else
                {
                    nextRunIn = toggleDiskAt - now;
                    break;
                }
            }
        }

        for (AutoDiskfulConfig cfg : cfgsToExecute)
        {
            try
            {
                Resource rsc = cfg.rsc;
                StateFlags<Resource.Flags> rscFlags = rsc.getStateFlags();
                if (
                    rscFlags.isSet(sysCtx, Resource.Flags.DRBD_DISKLESS) &&
                        !rscFlags.isSet(sysCtx, Resource.Flags.DISK_ADD_REQUESTED) &&
                        !rscFlags.isSet(sysCtx, Resource.Flags.DISK_ADDING)
                )
                {
                    Set<StorPool> autoPlace;
                    try (LinStorScope.ScopeAutoCloseable close = linstorScope.enter())
                    {
                        ResourceDefinition rscDfn = rsc.getResourceDefinition();
                        long sizeInKib = getSize(rscDfn);

                        linstorScope.seed(Key.get(AccessContext.class, PeerContext.class), sysCtx);

                        autoPlace = autoplacer.autoPlace(
                            AutoSelectFilterPojo.merge(
                                new AutoSelectFilterBuilder()
                                    .setPlaceCount(1)
                                    .setNodeNameList(Collections.singletonList(rsc.getNode().getName().displayValue))
                                    .setSkipAlreadyPlacedOnNodeNamesCheck(
                                        rscDfn.streamResource(sysCtx)
                                            .map(tmpRsc -> tmpRsc.getNode().getName().displayValue)
                                            .collect(Collectors.toList())
                                    )
                                    .build(),
                                rscDfn.getResourceGroup().getAutoPlaceConfig().getApiData()
                            ),
                            rscDfn,
                            sizeInKib
                        );
                    }
                    // need to exit scope here to ensure the following subscribe does not try to use the same scope

                    if (autoPlace == null || autoPlace.isEmpty())
                    {
                        errorReporter.logError(
                            "Failed to automatically make %s diskful as autoplacer failed to find suitable " +
                                "storage pool",
                            CtrlRscApiCallHandler.getRscDescription(rsc)
                        );
                    }
                    else
                    {
                        String storPoolNameStr = autoPlace.iterator().next().getName().displayValue;
                        toggleDisklHandler.resourceToggleDisk(
                            rsc.getNode().getName().displayValue,
                            rsc.getResourceDefinition().getName().displayValue,
                            storPoolNameStr,
                            null,
                            null, // TODO: could be a bad idea if not all layers from peer-resources are supported by
                                  // the local node...
                            false,
                            Resource.DiskfulBy.AUTO_DISKFUL
                        )
                            .concatWith(removeExcessFlux(rsc))
                            .contextWrite(
                                Context.of(
                                    ApiModule.API_CALL_NAME,
                                    "Auto diskful task for " + CtrlRscApiCallHandler.getRscDescription(rsc),
                                    AccessContext.class,
                                    sysCtx,
                                    Peer.class,
                                    rsc.getNode().getPeer(sysCtx)
                                )
                            )
                            .subscribe();
                    }
                }
            }
            catch (AccessDeniedException exc)
            {
                errorReporter.reportError(new ImplementationError(exc));
            }
        }
        return getNextFutureReschedule(scheduleAt, Math.min(nextRunIn, TASK_TIMEOUT));
    }

    private Mono<ApiCallRc> removeExcessFlux(Resource rsc)
    {
        var logContextMap = MDC.getCopyOfContextMap();
        return Mono.fromRunnable(
            () -> backgroundRunner.runInBackground(
                "Remove excess rsc",
                eventWaiter.waitForStream(
                    resourceStateEvent.get(),
                    ObjectIdentifier.resource(rsc.getNode().getName(), rsc.getResourceDefinition().getName())
                )
                    .skipUntil(usage -> usage.getUpToDate() != null && usage.getUpToDate())
                    .next()
                    .thenMany(
                        scopeRunner.fluxInTransactionalScope(
                            "Delete excess after auto-diskful of " + CtrlRscApiCallHandler.getRscDescription(rsc),
                            lockGuardFactory.buildDeferred(LockType.WRITE, LockObj.RSC_DFN_MAP),
                            () -> removeExcessFluxInTransaction(rsc),
                            logContextMap
                        )
                    )
                    .onErrorResume(PeerNotConnectedException.class, ignored -> Flux.empty())
            )
        );
    }

    private Flux<ApiCallRc> removeExcessFluxInTransaction(Resource rsc)
        throws InvalidKeyException, AccessDeniedException
    {
        Flux<ApiCallRc> retFlux;
        Resource excessRsc = getExcessRsc(rsc);

        if (excessRsc == null)
        {
            errorReporter.logDebug("Could not find resource to automatically cleanup after auto-diskful.");
            retFlux = Flux.empty();
        }
        else
        {
            errorReporter.logDebug(
                "Deleting excess %s after auto-diskful of %s",
                CtrlRscApiCallHandler.getRscDescription(excessRsc),
                CtrlRscApiCallHandler.getRscDescription(rsc)
            );
            retFlux = rscDeleteApiCallHandler.deleteResource(
                excessRsc.getNode().getName().displayValue,
                excessRsc.getResourceDefinition().getName().displayValue
            ).contextWrite(
                Context.of(
                    ApiModule.API_CALL_NAME,
                    "Deleting excess " + CtrlRscApiCallHandler.getRscDescription(excessRsc),
                    AccessContext.class,
                    sysCtx,
                    Peer.class,
                    excessRsc.getNode().getPeer(sysCtx)
                )
            );
        }
        return retFlux;
    }

    /**
     * This method returns a single resource (or null) that can be deleted after the given <code>rscRef</code> fully
     * synced up.
     *
     * Null is returned in the following cases:
     * <ul>
     * <li>The current diskful count of the rscDfn does not exceed the rscGrp's place-count</li>
     * <li>
     *  No resources is allowed to be cleaned up <br />
     *  A resource is allowed to be cleaned up if:
     *  <ul>
     *   <li>It became diskful via previous auto-diskful or make-availeble</li>
     *   <li>The resource is explicitly marked to be allowed to be cleaned up (via property
     *       {@value ApiConsts#NAMESPC_DRBD_OPTIONS}/{@value ApiConsts#KEY_DRBD_AUTO_DISKFUL_ALLOW_CLEANUP})</li>
     *  </ul>
     *  The given resource <code>rscRef</code> will never be returned.
     * </li>
     * </ul>
     *
     * @param rscRef
     * @return
     * @throws InvalidKeyException
     * @throws AccessDeniedException
     */
    private @Nullable Resource getExcessRsc(Resource rscRef) throws InvalidKeyException, AccessDeniedException
    {
        @Nullable Resource excessRsc = null;

        Set<Resource> fixedResources = new HashSet<>();
        fixedResources.add(rscRef);

        ResourceDefinition rscDfn = rscRef.getResourceDefinition();
        Integer replicaCount = rscDfn.getResourceGroup().getAutoPlaceConfig().getReplicaCount(sysCtx);
        if (replicaCount != null && replicaCount < rscDfn.getNotDeletedDiskfulCount(sysCtx))
        {
            Iterator<Resource> rscIt = rscDfn.iterateResource(sysCtx);
            while (rscIt.hasNext())
            {
                Resource rsc = rscIt.next();
                if (!rsc.equals(rscRef))
                {
                    String createdBy = rsc.getProps(sysCtx).getProp(ApiConsts.KEY_RSC_DISKFUL_BY);
                    boolean explicitlyAllowed = "true".equalsIgnoreCase(
                        getPrioProps(rsc).getProp(
                            ApiConsts.KEY_DRBD_AUTO_DISKFUL_ALLOW_CLEANUP,
                            ApiConsts.NAMESPC_DRBD_OPTIONS
                        )
                    );
                    if ((createdBy == null || DO_NOT_CLEANUP_TYPES.contains(createdBy)) && !explicitlyAllowed)
                    {
                        fixedResources.add(rsc);
                    }
                }
            }
        }

        excessRsc = autoUnplacer.unplace(rscDfn, fixedResources);

        return excessRsc;
    }

    private long getSize(ResourceDefinition rscDfnRef) throws AccessDeniedException
    {
        long sum = 0;
        Iterator<VolumeDefinition> vlmDfnIt = rscDfnRef.iterateVolumeDfn(sysCtx);
        while (vlmDfnIt.hasNext())
        {
            VolumeDefinition vlmDfn = vlmDfnIt.next();
            sum += vlmDfn.getVolumeSize(sysCtx);
        }
        return sum;
    }

    private class AutoDiskfulConfig implements Comparable<AutoDiskfulConfig>
    {
        final long disklessPrimarySince;
        final Resource rsc;
        long toggleDiskAfter;

        AutoDiskfulConfig(long disklessPrimarySinceRef, Resource rscRef, long toggleDiskAfterRef)
        {
            disklessPrimarySince = disklessPrimarySinceRef;
            rsc = rscRef;
            toggleDiskAfter = toggleDiskAfterRef;
        }

        @Override
        public int compareTo(AutoDiskfulConfig other)
        {
            int cmp = Long.compare(
                disklessPrimarySince + toggleDiskAfter,
                other.disklessPrimarySince + other.toggleDiskAfter
            );
            if (cmp == 0)
            {
                cmp = rsc.compareTo(other.rsc);
            }
            return cmp;
        }
    }
}
