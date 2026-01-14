package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.SharedResourceManager;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller.NotConnectedHandler;
import com.linbit.linstor.core.apicallhandler.controller.utils.ResourceDataUtils;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.apicallhandler.response.ResponseUtils;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.resource.CtrlRscLayerDataFactory;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.tasks.AutoSnapshotTask;
import com.linbit.linstor.tasks.ScheduleBackupService;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.utils.ExceptionThrowingFunction;
import com.linbit.utils.ExceptionThrowingPredicate;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscDfnApiCallHandler.getRscDfnDescription;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscDfnApiCallHandler.getRscDfnDescriptionInline;
import static com.linbit.utils.StringUtils.firstLetterCaps;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import reactor.core.publisher.Flux;

/**
 * API call handler to truncate all {@link Resource}s of a given {@link ResourceDefinition}.
 * This class takes care of the following things before/while truncating:
 * <ul>
 * <li>No resource is allowed to be "in use"</li>
 * <li>Diskless resources must be deleted before diskful resources</li>
 * <li>Since no resources will be left after truncation, the ResourceDefinition is deregisterd from
 * <ul>
 * <li>{@link ScheduleBackupService}</li>
 * <li>{@link AutoSnapshotTask}</li>
 * </ul>
 * </li>
 * </ul>
 */
@Singleton
public class CtrlRscDfnTruncateApiCallHandler
{
    private final AccessContext apiCtx;
    private final Provider<AccessContext> peerAccCtxProvider;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final SharedResourceManager sharedRscMgr;
    private final ScheduleBackupService scheduleService;
    private final AutoSnapshotTask autoSnapshotTask;
    private final CtrlRscActivateApiCallHandler ctrlRscActivateApiCallHandler;
    private final CtrlRscDeleteApiHelper ctrlRscDeleteApiHelper;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlRscLayerDataFactory ctrlRscLayerDataFactory;
    private final CtrlResyncAfterHelper ctrlResyncAfterHelper;
    private final ErrorReporter errorReporter;
    private final ResponseConverter responseConverter;

    @Inject
    public CtrlRscDfnTruncateApiCallHandler(
        @ApiContext AccessContext apiCtxRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        SharedResourceManager sharedRscMgrRef,
        ScheduleBackupService scheduleServiceRef,
        AutoSnapshotTask autoSnapshotTaskRef,
        CtrlRscActivateApiCallHandler ctrlRscActivateApiCallHandlerRef,
        CtrlRscDeleteApiHelper ctrlRscDeleteApiHelperRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlRscLayerDataFactory ctrlRscLayerDataFactoryRef,
        CtrlResyncAfterHelper ctrlResyncAfterHelperRef,
        ErrorReporter errorReporterRef,
        ResponseConverter responseConverterRef
    )
    {
        apiCtx = apiCtxRef;
        peerAccCtxProvider = peerAccCtxRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        sharedRscMgr = sharedRscMgrRef;
        scheduleService = scheduleServiceRef;
        autoSnapshotTask = autoSnapshotTaskRef;
        ctrlRscActivateApiCallHandler = ctrlRscActivateApiCallHandlerRef;
        ctrlRscDeleteApiHelper = ctrlRscDeleteApiHelperRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlRscLayerDataFactory = ctrlRscLayerDataFactoryRef;
        ctrlResyncAfterHelper = ctrlResyncAfterHelperRef;
        errorReporter = errorReporterRef;
        responseConverter = responseConverterRef;
    }

    public Flux<ApiCallRc> truncateRscDfn(ResourceName rscName, boolean suppressNodeOfflineWarningRef)
    {
        ResponseContext context = CtrlRscDfnApiCallHandler.makeResourceDefinitionContext(
            ApiOperation.makeDeleteOperation(),
            rscName.displayValue
        );

        return scopeRunner
            .fluxInTransactionalScope(
                "Truncate resource definition",
                lockGuardFactory.create().write(LockObj.RSC_DFN_MAP).buildDeferred(),
                () -> truncateRscDfnInTransaction(rscName, suppressNodeOfflineWarningRef)
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    /**
     * Truncates a resource definition, that means this method first deletes all diskless resources and afterwards the
     * remaining resources.<br />
     * The resource definition itself will NOT be removed by this method.
     *
     * @param rscName The resource definition's name that should be truncated.
     * @param suppressNodeOfflineWarningRef if a node if offline a Flux.error will be triggered with and ApiRcException
     * containing a warning that the node is offline. If this parameter is set to true, that error flux is consumed and
     * ignored with a Flux.empty().
     */
    public Flux<ApiCallRc> truncateRscDfnInTransaction(ResourceName rscName, boolean suppressNodeOfflineWarningRef)
    {
        ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(rscName, false);
        if (rscDfn == null)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.WARN_NOT_FOUND,
                    getRscDfnDescription(rscName) + " not found."
                )
            );
        }

        ensureNoRscInUse(rscDfn);

        Flux<ApiCallRc> flux;
        scheduleService.removeTasks(rscDfn);
        autoSnapshotTask.removeAutoSnapshotting(rscName.getName());

        ctrlTransactionHelper.commit();

        if (rscDfn.getResourceCount() > 0)
        {
            String descriptionFirstLetterCaps = firstLetterCaps(getRscDfnDescriptionInline(rscDfn));
            ApiCallRc responses = ApiCallRcImpl.singletonApiCallRc(
                ApiCallRcImpl.entryBuilder(ApiConsts.DELETED, descriptionFirstLetterCaps + " marked for deletion.")
                    .build()
            );

            AccessContext peerAccCtx = peerAccCtxProvider.get();

            flux = Flux.just(responses);
            try
            {
                if (rscDfn.hasDiskless(peerAccCtx))
                {
                    // first delete diskless resources, because DRBD raises an error if all peers with disks are
                    // removed from a diskless resource
                    TruncateContext truncateDisklessRscs = new TruncateContext(
                        rscName,
                        rsc -> rsc.isDiskless(peerAccCtx),
                        null,
                        this::markDisklessForDeletion,
                        rsc -> deleteRsc(rsc, false),
                        "Deleting diskless resources"
                    );
                    flux = flux.concatWith(deleteResources(truncateDisklessRscs));
                }
            }
            catch (AccessDeniedException exc)
            {
                throw new ApiAccessDeniedException(
                    exc,
                    "checking if resource-definition has diskless resources",
                    ApiConsts.FAIL_ACC_DENIED_RSC_DFN
                );
            }

            TruncateContext truncateRemainingRscs = new TruncateContext(
                rscName,
                ignore -> true,
                this::updateSnapPropChanges,
                this::markRemainingForDeletion,
                rsc -> deleteRsc(rsc, true),
                "Deleting remaining resources"
            );
            flux = flux.concatWith(deleteResources(truncateRemainingRscs));

            if (suppressNodeOfflineWarningRef)
            {
                flux = flux.onErrorResume(CtrlResponseUtils.DelayedApiRcException.class, ignored -> Flux.empty());
            }
        }
        else
        {
            flux = Flux.empty();
        }
        return flux;
    }

    public void ensureNoRscInUse(ResourceDefinition rscDfn)
    {
        ensureNoRscInUsePriviledged(apiCtx, rscDfn);
    }

    public static void ensureNoRscInUsePriviledged(AccessContext apiCtx, ResourceDefinition rscDfn)
    {
        try
        {
            Optional<Resource> rscInUse = rscDfn.anyResourceInUse(apiCtx);
            if (rscInUse.isPresent())
            {
                NodeName nodeName = rscInUse.get().getNode().getName();
                throw new ApiRcException(
                    ApiCallRcImpl
                        .entryBuilder(
                            ApiConsts.FAIL_IN_USE,
                            String.format(
                                "Resource '%s' on node '%s' is still in use.",
                                rscDfn.getName().displayValue,
                                nodeName.displayValue
                            )
                        )
                        .setCause("Resource is mounted/in use.")
                        .setCorrection(
                            String.format(
                                "Un-mount resource '%s' on the node '%s'.",
                                rscDfn.getName().displayValue,
                                nodeName.displayValue
                            )
                        )
                        .setSkipErrorReport(true)
                        .build()
                );
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private Flux<ApiCallRc> deleteResources(TruncateContext truncateCtxRef)
    {
        Flux<ApiCallRc> ret;
        Flux<ApiCallRc> markAndDeleteFluxes = deleteResourcesMultiStep(
            truncateCtxRef,
            ". Step 1 of 2",
            truncateCtxRef.markForDeleteAction,
            "Resource {1} on {0} marked for deletion",
            deleteResourcesMultiStep(
                truncateCtxRef,
                ". Step 2 of 2",
                truncateCtxRef.deleteAction,
                "Resource {1} on {0} deleted",
                Flux.empty()
            )
        );
        if (truncateCtxRef.preMarkForDeleteAction != null)
        {
            ret = runPreMarkDeleteFlux(truncateCtxRef)
                .concatWith(markAndDeleteFluxes);
        }
        else
        {
            ret = markAndDeleteFluxes;
        }
        return ret;
    }

    private Flux<ApiCallRc> runPreMarkDeleteFlux(TruncateContext truncateCtxRef)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                truncateCtxRef.scopeDescription + " preparing deletion",
                lockGuardFactory.create().write(LockObj.RSC_DFN_MAP).buildDeferred(),
                () -> runPreMarkDeleteFluxInTx(
                    truncateCtxRef
                )
            );
    }

    private Flux<ApiCallRc> runPreMarkDeleteFluxInTx(TruncateContext truncateCtxRef)
    {
        Flux<ApiCallRc> flux;
        @Nullable ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(truncateCtxRef.rscName, false);
        if (rscDfn != null)
        {
            try
            {
                @Nullable Flux<ApiCallRc> preMarkDeleteFlux = truncateCtxRef.preMarkForDeleteAction.accept(rscDfn);
                if (preMarkDeleteFlux != null)
                {
                    flux = preMarkDeleteFlux;
                }
                else
                {
                    flux = Flux.empty();
                }
            }
            catch (AccessDeniedException exc)
            {
                throw new ApiAccessDeniedException(
                    exc,
                    truncateCtxRef.scopeDescription + " preparing deletion",
                    ApiConsts.FAIL_ACC_DENIED_RSC
                );
            }
        }
        else
        {
            flux = Flux.just(
                ApiCallRcImpl.singleApiCallRc(
                    ApiConsts.INFO_NOOP,
                    getRscDfnDescription(truncateCtxRef.rscName) + " does not exist. Noop."
                )
            );
        }
        return flux;
    }

    private Flux<ApiCallRc> deleteResourcesMultiStep(
        TruncateContext truncateCtxRef,
        String subScopeDescrRef,
        ExceptionThrowingFunction<Resource, /* Nullable */ Flux<ApiCallRc>, AccessDeniedException> actionRef,
        String updateStltsFormatRef,
        Flux<ApiCallRc> nextStepRef
    )
    {
        return scopeRunner
            .fluxInTransactionalScope(
                truncateCtxRef.scopeDescription + subScopeDescrRef,
                lockGuardFactory.create().write(LockObj.RSC_DFN_MAP).buildDeferred(),
                () -> deleteResourcesMultiStepInTx(
                    truncateCtxRef,
                    subScopeDescrRef,
                    actionRef,
                    updateStltsFormatRef,
                    nextStepRef
                )
            );
    }

    private Flux<ApiCallRc> deleteResourcesMultiStepInTx(
        TruncateContext truncateCtxRef,
        String subScopeDescrRef,
        ExceptionThrowingFunction<Resource, /* TODO: @Nullable */ Flux<ApiCallRc>, AccessDeniedException> actionRef,
        String updateStltsFormatRef,
        Flux<ApiCallRc> nextStepRef
    )
    {
        Flux<ApiCallRc> flux;
        @Nullable ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(truncateCtxRef.rscName, false);
        if (rscDfn != null)
        {
            try
            {
                flux = Flux.empty();
                ExceptionThrowingPredicate<Resource, AccessDeniedException> predicate = truncateCtxRef.rscFilter;

                // copy resources into dedicated map / collection since the actionRef might delete the resources, which
                // would cause us here a ConcurrentModificationException
                Map<NodeName, Resource> copy = new HashMap<>();

                if (truncateCtxRef.preMarkForDeleteAction != null)
                {
                    @Nullable Flux<ApiCallRc> preMarkFlux = truncateCtxRef.preMarkForDeleteAction.accept(rscDfn);
                    if (preMarkFlux != null)
                    {
                        flux = flux.concatWith(preMarkFlux);
                    }
                }

                rscDfn.copyResourceMap(peerAccCtxProvider.get(), copy);
                for (Resource rsc : copy.values())
                {
                    if (predicate.test(rsc))
                    {
                        @Nullable Flux<ApiCallRc> actionFlux = actionRef.accept(rsc);
                        if (actionFlux != null)
                        {
                            flux = flux.concatWith(actionFlux);
                        }
                    }
                }

                ctrlTransactionHelper.commit();

                flux = flux.concatWith(
                    ctrlSatelliteUpdateCaller.updateSatellites(
                        rscDfn,
                        nodeName -> Flux.error(new ApiRcException(ResponseUtils.makeNotConnectedWarning(nodeName))),
                        nextStepRef
                    )
                        .transform(
                            updateResponses -> CtrlResponseUtils.combineResponses(
                                errorReporter,
                                updateResponses,
                                truncateCtxRef.rscName,
                                updateStltsFormatRef
                            )
                        )
                ).concatWith(nextStepRef);
            }
            catch (AccessDeniedException exc)
            {
                throw new ApiAccessDeniedException(
                    exc,
                    truncateCtxRef.scopeDescription + subScopeDescrRef,
                    ApiConsts.FAIL_ACC_DENIED_RSC
                );
            }
        }
        else
        {
            flux = Flux.just(
                ApiCallRcImpl.singleApiCallRc(
                    ApiConsts.INFO_NOOP,
                    getRscDfnDescription(truncateCtxRef.rscName) + " does not exist. Noop."
                )
            );
        }
        return flux;
    }

    private void markForDeletion(Resource rscRef)
    {
        // TODO: shouldn't we first set DRBD_DELETE here? like
        // CtrlRscDeleteApiCallHandler.prepareREsourceDeleteInTransaction does?
        try
        {
            AccessContext peerAccCtx = peerAccCtxProvider.get();
            rscRef.markDeleted(peerAccCtx);
            Iterator<Volume> volumesIterator = rscRef.iterateVolumes();
            while (volumesIterator.hasNext())
            {
                Volume vlm = volumesIterator.next();
                vlm.markDeleted(apiCtx);
            }
            ResourceDataUtils.recalculateVolatileRscData(ctrlRscLayerDataFactory, rscRef);
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(exc, "mark resource for deletion", ApiConsts.FAIL_ACC_DENIED_RSC);
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
    }

    private Flux<ApiCallRc> markDisklessForDeletion(Resource rscRef)
    {
        markForDeletion(rscRef);
        return Flux.empty();
    }

    private Flux<ApiCallRc> updateSnapPropChanges(ResourceDefinition rscDfnRef)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Updating snapDfn properties if needed before deleting resources",
                lockGuardFactory.create().write(LockObj.RSC_DFN_MAP).buildDeferred(),
                () -> updateSnapPropChangesInTx(
                    rscDfnRef
                )
            );
    }

    private Flux<ApiCallRc> updateSnapPropChangesInTx(ResourceDefinition rscDfnRef)
    {
        Flux<ApiCallRc> flux = Flux.empty();
        Set<SnapshotDefinition> snapDfnsToUpdate = new HashSet<>();
        Iterator<Resource> rscIt;
        try
        {
            rscIt = rscDfnRef.iterateResource(peerAccCtxProvider.get());
            while (rscIt.hasNext())
            {
                Resource rsc = rscIt.next();
                snapDfnsToUpdate.addAll(
                    CtrlRscDeleteApiCallHandler.handleZfsRenameIfNeeded(
                        apiCtx,
                        rsc
                    )
                );
            }

            ctrlTransactionHelper.commit();

            ResourceName rscName = rscDfnRef.getName();
            NotConnectedHandler notConnectedHandler = nodeName -> Flux.error(
                new ApiRcException(ResponseUtils.makeNotConnectedWarning(nodeName))
            );
            // TODO re really need an atomic updater...
            for (SnapshotDefinition snapDfnToUpdate : snapDfnsToUpdate)
            {
                flux = flux.concatWith(
                    ctrlSatelliteUpdateCaller.updateSatellites(snapDfnToUpdate, notConnectedHandler)
                        .transform(
                            updateResponses -> CtrlResponseUtils.combineResponses(
                                errorReporter,
                                updateResponses,
                                rscName,
                                "SnapshotDefinition {1} on {0} updated"
                            )
                        )
                );
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "Updating snapshot properties before deleting resources",
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }
        return flux;
    }

    private Flux<ApiCallRc> markRemainingForDeletion(Resource rscRef) throws AccessDeniedException
    {
        Flux<ApiCallRc> flux = Flux.empty();
        StateFlags<Flags> rscFlags = rscRef.getStateFlags();
        if (rscFlags.isSet(apiCtx, Resource.Flags.INACTIVE))
        {
            @Nullable Resource rscToActivate = null;
            if (!rscFlags.isSet(apiCtx, Resource.Flags.INACTIVE_PERMANENTLY))
            {
                rscToActivate = rscRef;
            }
            @Nullable Resource activeRsc = null;
            for (Resource sharedRsc : sharedRscMgr.getSharedResources(rscRef))
            {
                StateFlags<Flags> sharedRscFlags = sharedRsc.getStateFlags();
                if (!sharedRscFlags.isSet(apiCtx, Resource.Flags.INACTIVE))
                {
                    activeRsc = sharedRsc;
                    break;
                }
                if (!sharedRscFlags.isSet(apiCtx, Resource.Flags.INACTIVE_PERMANENTLY))
                {
                    rscToActivate = sharedRsc;
                }
            }
            if (activeRsc == null && rscToActivate != null)
            {
                flux = ctrlRscActivateApiCallHandler.activateRsc(
                    rscToActivate.getNode().getName().displayValue,
                    rscToActivate.getResourceDefinition().getName().displayValue
                );
            }
        }
        markForDeletion(rscRef);
        return flux;
    }

    private @Nullable Flux<ApiCallRc> deleteRsc(Resource rscRef, boolean runResyncAfterHelper)
    {
        ctrlRscDeleteApiHelper.cleanupAndDelete(rscRef);
        @Nullable Flux<ApiCallRc> ret = null;
        if (runResyncAfterHelper)
        {
            ret = ctrlResyncAfterHelper.fluxManage();
        }
        return ret;
    }

    private static class TruncateContext
    {
        private final ResourceName rscName;
        private final ExceptionThrowingPredicate<Resource, AccessDeniedException> rscFilter;
        @SuppressWarnings("LineLength")
        private final @Nullable ExceptionThrowingFunction<ResourceDefinition, Flux<ApiCallRc>, AccessDeniedException> preMarkForDeleteAction;
        private final ExceptionThrowingFunction<Resource, Flux<ApiCallRc>, AccessDeniedException> markForDeleteAction;
        private final ExceptionThrowingFunction<Resource, Flux<ApiCallRc>, AccessDeniedException> deleteAction;
        private final String scopeDescription;

        private TruncateContext(
            ResourceName rscNameRef,
            ExceptionThrowingPredicate<Resource, AccessDeniedException> rscFilterRef,
            @SuppressWarnings("LineLength")
            @Nullable ExceptionThrowingFunction<ResourceDefinition, Flux<ApiCallRc>, AccessDeniedException> preMarkForDeleteActionRef,
            ExceptionThrowingFunction<Resource, Flux<ApiCallRc>, AccessDeniedException> markForDeleteActionRef,
            ExceptionThrowingFunction<Resource, Flux<ApiCallRc>, AccessDeniedException> deleteActionRef,
            String scopeDescriptionRef
        )
        {
            rscName = rscNameRef;
            rscFilter = rscFilterRef;
            preMarkForDeleteAction = preMarkForDeleteActionRef;
            markForDeleteAction = markForDeleteActionRef;
            deleteAction = deleteActionRef;
            scopeDescription = scopeDescriptionRef;
        }
    }
}
