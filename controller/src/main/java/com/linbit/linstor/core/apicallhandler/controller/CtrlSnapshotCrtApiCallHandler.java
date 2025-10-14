package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.TimeoutException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.BackgroundRunner;
import com.linbit.linstor.core.BackgroundRunner.RunConfig;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.controller.internal.helpers.AtomicUpdateSatelliteData;
import com.linbit.linstor.core.apicallhandler.controller.req.CreateMultiSnapRequest;
import com.linbit.linstor.core.apicallhandler.controller.req.CreateMultiSnapRequest.SnapReq;
import com.linbit.linstor.core.apicallhandler.controller.utils.SatelliteResourceStateDrbdUtils;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSuccessUtils;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.ebs.EbsStatusManagerService;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.storage.ebs.EbsUtils;
import com.linbit.linstor.layer.utils.SuspendLayerUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotApiCallHandler.getSnapshotDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotApiCallHandler.makeSnapshotContext;
import static com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller.notConnectedError;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.slf4j.MDC;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;

@Singleton
public class CtrlSnapshotCrtApiCallHandler
{
    private final AccessContext apiCtx;
    private final ScopeRunner scopeRunner;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final ResponseConverter responseConverter;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlPropsHelper propsHelper;

    private final CtrlSnapshotCrtHelper ctrlSnapshotCrtHelper;
    private final ErrorReporter errorReporter;
    private final CtrlSnapshotDeleteApiCallHandler ctrlSnapshotDeleteApiCallHandler;
    private final EbsStatusManagerService ebsStatusMgr;
    private final BackgroundRunner backgroundRunner;

    @Inject
    public CtrlSnapshotCrtApiCallHandler(
        @ApiContext AccessContext apiCtxRef,
        ScopeRunner scopeRunnerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        ResponseConverter responseConverterRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlSnapshotCrtHelper ctrlSnapshotCrtHelperRef,
        CtrlPropsHelper propsHelperRef,
        ErrorReporter errorReporterRef,
        CtrlSnapshotDeleteApiCallHandler ctrlSnapshotDeleteApiCallHandlerRef,
        EbsStatusManagerService ebsStatusManagerServiceRef,
        BackgroundRunner backgroundRunnerRef
    )
    {
        apiCtx = apiCtxRef;
        scopeRunner = scopeRunnerRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        responseConverter = responseConverterRef;
        lockGuardFactory = lockGuardFactoryRef;
        ctrlSnapshotCrtHelper = ctrlSnapshotCrtHelperRef;
        propsHelper = propsHelperRef;
        errorReporter = errorReporterRef;
        ctrlSnapshotDeleteApiCallHandler = ctrlSnapshotDeleteApiCallHandlerRef;
        ebsStatusMgr = ebsStatusManagerServiceRef;
        backgroundRunner = backgroundRunnerRef;
    }

    /**
     * Create a snapshot of a resource.
     * <p>
     * Snapshots are created in a multi-stage process:
     * <ol>
     * <li>Add the snapshot objects (definition and instances), marked with the suspend flag</li>
     * <li>When all resources are suspended, send out a snapshot request</li>
     * <li>When all snapshots have been created, mark the resource as resuming by removing the suspend flag</li>
     * <li>When all resources have been resumed, remove the in-progress state of the snapshots</li>
     * </ol>
     */
    public Flux<ApiCallRc> createSnapshot(
        List<String> nodeNameStrs,
        String rscNameStr,
        String snapshotNameStr,
        Map<String, String> props
    )
    {
        return createSnapshot(nodeNameStrs, rscNameStr, snapshotNameStr, props, true);
    }

    /**
     * Create a snapshot of a resource.
     * <p>
     * Snapshots are created in a multi-stage process:
     * <ol>
     * <li>Add the snapshot objects (definition and instances), marked with the suspend flag</li>
     * <li>When all resources are suspended, send out a snapshot request</li>
     * <li>When all snapshots have been created, mark the resource as resuming by removing the suspend flag</li>
     * <li>When all resources have been resumed, remove the in-progress snapshots</li>
     * </ol>
     */
    public Flux<ApiCallRc> createSnapshot(
        List<String> nodeNameStrs,
        String rscNameStr,
        String snapshotNameStr,
        Map<String, String> props,
        boolean handleErrors
    )
    {
        return createSnapshot(
            singleSnapReq(nodeNameStrs, rscNameStr, snapshotNameStr, props),
            handleErrors
        );
    }

    private CreateMultiSnapRequest singleSnapReq(
        List<String> nodeNameStrsRef,
        String rscNameStrRef,
        String snapshotNameStrRef,
        Map<String, String> props
    )
    {
        return new CreateMultiSnapRequest(
            Collections.singleton(new SnapReq(nodeNameStrsRef, rscNameStrRef, snapshotNameStrRef, props))
        );
    }

    public Flux<ApiCallRc> createSnapshot(CreateMultiSnapRequest req){
        return createSnapshot(req, true);
    }

    public Flux<ApiCallRc> createSnapshot(CreateMultiSnapRequest req, boolean handleErrors)
    {
        Flux<ApiCallRc> fluxInTransactionalScope = scopeRunner
            .fluxInTransactionalScope(
                "Create (multi) snapshot",
                lockGuardFactory.create()
                    .read(LockObj.NODES_MAP)
                    .write(LockObj.RSC_DFN_MAP)
                    .buildDeferred(),
                () -> createSnapshotInTransaction(req)
            );
        if (handleErrors)
        {
            fluxInTransactionalScope = fluxInTransactionalScope.transform(
                responses -> responseConverter.reportingExceptions(req.makeSnapshotContext(), responses)
            );
        }
        return fluxInTransactionalScope;

    }

    private Flux<ApiCallRc> createSnapshotInTransaction(CreateMultiSnapRequest req)
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();

        List<SnapshotDefinition> snapDfnList = new ArrayList<>();
        for (SnapReq snapReq : req.getSnapRequests())
        {
            List<String> nodeNameStrs = snapReq.getNodeNames();
            String rscNameStr = snapReq.getRscName();
            String snapshotNameStr = snapReq.getSnapName();
            SnapshotDefinition snapDfn = ctrlSnapshotCrtHelper.createSnapshots(
                nodeNameStrs,
                LinstorParsingUtils.asRscName(rscNameStr),
                LinstorParsingUtils.asSnapshotName(snapshotNameStr),
                snapReq.getProps(),
                responses
            );
            snapDfnList.add(snapDfn);
            responses.addEntry(
                ApiSuccessUtils.defaultRegisteredEntry(
                    snapDfn.getUuid(),
                    getSnapshotDescriptionInline(
                        nodeNameStrs,
                        rscNameStr,
                        snapshotNameStr
                    )
                )
            );
        }

        req.setCreatedSnapDfns(snapDfnList);
        ctrlTransactionHelper.commit();

        return Flux.<ApiCallRc>just(responses)
            .concatWith(postCreateSnapshot(req, false));
    }

    /**
     * Create a snapshot of a resource.
     * <p>
     * Snapshots are created in a multi-stage process:
     * <ol>
     * <li>Add the snapshot objects (definition and instances), marked with the suspend flag</li>
     * <li>When all resources are suspended, send out a snapshot request</li>
     * <li>When all snapshots have been created, mark the resource as resuming by removing the suspend flag</li>
     * <li>When all resources have been resumed, remove the in-progress snapshots</li>
     * </ol>
     */
    public Flux<ApiCallRc> createAutoSnapshot(
        List<String> nodeNameStrs,
        String rscNameStr
    )
    {
        ResponseContext context = makeSnapshotContext(
            ApiOperation.makeCreateOperation(),
            nodeNameStrs,
            rscNameStr,
            "auto"
        );

        return scopeRunner.fluxInTransactionalScope(
            "Create snapshot",
            lockGuardFactory.create()
                .read(LockObj.NODES_MAP)
                .write(LockObj.RSC_DFN_MAP)
                .buildDeferred(),
            () -> createAutoSnapshotInTransaction(nodeNameStrs, rscNameStr)
        ).transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> createAutoSnapshotInTransaction(
        List<String> nodeNameStrs,
        String rscNameStr
    )
    {
        Flux<ApiCallRc> flux;
        ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(rscNameStr, true);

        List<String> offlineNodeName = getOfflineNodeNames(rscDfn);
        if (!offlineNodeName.isEmpty())
        {
            errorReporter.logWarning(
                "Skipping auto-snapshot for resource '%s', as nodes %s are offline",
                rscDfn.getName().displayValue,
                offlineNodeName
            );
            flux = Flux.empty();
        }
        else
        {
            String autoSnapshotName;
            SnapshotDefinition snapDfn;
            do
            {
                autoSnapshotName = getAutoSnapshotName(rscDfn);
                try
                {
                    snapDfn = rscDfn.getSnapshotDfn(apiCtx, new SnapshotName(autoSnapshotName));
                }
                catch (AccessDeniedException | InvalidNameException exc)
                {
                    throw new ImplementationError(exc);
                }
            }
            while (snapDfn != null);

            ApiCallRcImpl responses = new ApiCallRcImpl();

            snapDfn = ctrlSnapshotCrtHelper.createSnapshots(
                nodeNameStrs,
                LinstorParsingUtils.asRscName(rscNameStr),
                LinstorParsingUtils.asSnapshotName(autoSnapshotName),
                Collections.emptyMap(),
                responses
            );
            enableFlagPrivileged(snapDfn, SnapshotDefinition.Flags.AUTO_SNAPSHOT);

            ctrlTransactionHelper.commit();

            responses.addEntry(
                ApiSuccessUtils.defaultRegisteredEntry(
                    snapDfn.getUuid(),
                    getSnapshotDescriptionInline(nodeNameStrs, rscNameStr, autoSnapshotName)
                )
            );
            flux = Flux.<ApiCallRc>just(responses)
                .concatWith(postCreateSnapshot(createSimpleSnapshotRequestPrivileged(snapDfn), true));
        }
        return flux;
    }

    private List<String> getOfflineNodeNames(ResourceDefinition rscDfnRef)
    {
        List<String> offlineNodes = new ArrayList<>();
        Iterator<Resource> rscIt;
        try
        {
            rscIt = rscDfnRef.iterateResource(apiCtx);
            while (rscIt.hasNext())
            {
                Resource rsc = rscIt.next();
                Node node = rsc.getNode();
                if (!node.getPeer(apiCtx).isOnline())
                {
                    offlineNodes.add(node.getName().displayValue);
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return offlineNodes;
    }

    private String getAutoSnapshotName(ResourceDefinition rscDfnRef)
    {
        Props rscDfnProps = propsHelper.getProps(rscDfnRef);
        PriorityProps prioProps = new PriorityProps(
            rscDfnProps,
            propsHelper.getProps(rscDfnRef.getResourceGroup()),
            propsHelper.getCtrlPropsForView()
        );

        String snapPrefix = prioProps.getProp(
            ApiConsts.KEY_AUTO_SNAPSHOT_PREFIX,
            ApiConsts.NAMESPC_AUTO_SNAPSHOT,
            InternalApiConsts.DEFAULT_AUTO_SNAPSHOT_PREFIX
        );

        int id = Integer.parseInt(
            rscDfnProps.getPropWithDefault(
                ApiConsts.KEY_AUTO_SNAPSHOT_NEXT_ID,
                ApiConsts.NAMESPC_AUTO_SNAPSHOT,
                "0"
            )
        );
        id++;
        try
        {
            rscDfnProps.setProp(
                ApiConsts.KEY_AUTO_SNAPSHOT_NEXT_ID,
                Integer.toString(id),
                ApiConsts.NAMESPC_AUTO_SNAPSHOT
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "increment auto snapshot id of " + CtrlRscDfnApiCallHandler.getRscDfnDescription(rscDfnRef),
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
        catch (InvalidKeyException | InvalidValueException exc)
        {
            throw new ImplementationError(exc);
        }

        return String.format("%s%05d", snapPrefix, id);
    }

    @SuppressWarnings("unchecked")
    public Flux<ApiCallRc> postCreateSnapshot(SnapshotDefinition snapDfn, boolean runInBackgroundRef)
    {
        return postCreateSnapshotSuppressingErrorClasses(
            createSimpleSnapshotRequestPrivileged(snapDfn),
            runInBackgroundRef,
            CtrlResponseUtils.DelayedApiRcException.class
        );
    }

    @SuppressWarnings("unchecked")
    public Flux<ApiCallRc> postCreateSnapshot(CreateMultiSnapRequest reqRef, boolean runInBackgroundRef)
    {
        return postCreateSnapshotSuppressingErrorClasses(
            reqRef,
            runInBackgroundRef,
            CtrlResponseUtils.DelayedApiRcException.class
        );
    }

    @SuppressWarnings("unchecked")
    public <E extends Throwable> Flux<ApiCallRc> postCreateSnapshotSuppressingErrorClasses(
        SnapshotDefinition snapDfnRef,
        boolean runInBackgroundRef,
        Class<E>... suppressErrorClasses
        )
    {
        return postCreateSnapshotSuppressingErrorClasses(
            createSimpleSnapshotRequestPrivileged(snapDfnRef),
            runInBackgroundRef,
            suppressErrorClasses
        );
    }
    /**
     * After Snapshots with their SnapshotDefinition were created, this method now sends the update
     * to the corresponding satellites and also takes care of resuming-io in the end.
     *
     * @param snapshotDfn
     *
     * @return
     */
    @SuppressWarnings("unchecked")
    public <E extends Throwable> Flux<ApiCallRc> postCreateSnapshotSuppressingErrorClasses(
        CreateMultiSnapRequest reqRef,
        boolean runInBackgroundRef,
        Class<E>... suppressErrorClasses
    )
    {
        final List<NodeName> nodeNamesToLock = new ArrayList<>();
        {
            boolean reqNeedsUpdate = false;
            for (SnapshotDefinition snapDfn : reqRef.getCreatedSnapDfns())
            {
                if (!snapDfn.isDeleted())
                {
                    for (Snapshot snap : getAllSnapshotsPrivileged(snapDfn))
                    {
                        if (!snap.isDeleted())
                        {
                            nodeNamesToLock.add(snap.getNodeName());
                        }
                    }
                }
                else
                {
                    reqNeedsUpdate = true;
                }
            }
            if (reqNeedsUpdate)
            {
                updateRequestPrivileged(reqRef);
            }
        }

        var logContextMap = MDC.getCopyOfContextMap();
        // we create a supplier to avoid code-duplication.
        Supplier<Flux<ApiCallRc>> fluxSupplier = () ->
        {
            Flux<ApiCallRc> flux = ctrlSatelliteUpdateCaller
                .updateSatellites(reqRef, notConnectedError())
                .transform(
                    updateResponses -> CtrlResponseUtils.combineResponses(
                        errorReporter,
                        updateResponses,
                        reqRef.getJoinedRscNames(),
                        "Suspended IO of {1} on {0} for snapshot"
                    )
                );
            flux = flux.concatWith(takeSnapshot(reqRef))
                .onErrorResume(exception -> abortSnapshot(reqRef, exception));
            for (Class<E> clazz : suppressErrorClasses)
            {
                flux = flux.onErrorResume(clazz, ignored -> Flux.empty());
            }
            return flux;
        };

        Flux<ApiCallRc> flux;
        if (!runInBackgroundRef)
        {
            flux = backgroundRunner.syncWithBackgroundFluxes(
                new RunConfig<>(
                    0,
                    "Syncing with background tasks for " + reqRef.getDescription(),
                    () -> scopeRunner.fluxInTransactionalScope(
                        "Running queued snapshot",
                        lockGuardFactory.buildDeferred(LockType.WRITE, LockObj.NODES_MAP, LockObj.RSC_DFN_MAP),
                        // we still need to wrap the supplier in a new transactionalScope since otherwise the supplied
                        // flux will be executed without the scope.
                        fluxSupplier::get,
                        logContextMap
                    ),
                    apiCtx,
                    nodeNamesToLock,
                    false
                )
            );
        }
        else
        {
            flux = fluxSupplier.get();
        }
        return flux;
    }

    private Flux<ApiCallRc> abortSnapshot(
        CreateMultiSnapRequest reqRef,
        Throwable exception
    )
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Abort taking MultiSnapshot",
                lockGuardFactory.create()
                    .read(LockObj.NODES_MAP)
                    .write(LockObj.RSC_DFN_MAP)
                    .buildDeferred(),
                () -> abortSnapshotInTransaction(reqRef, exception),
                MDC.getCopyOfContextMap()
            );
    }

    private Flux<ApiCallRc> abortSnapshotInTransaction(
        CreateMultiSnapRequest reqRef,
        Throwable exception
    )
    {
        // no need for special handling for multi snapshot. Just concat aborting snapshots as if they were single
        // snapshots
        Flux<ApiCallRc> ret = Flux.empty();
        boolean updated = false;
        for (SnapshotDefinition snapDfn : reqRef.getCreatedSnapDfns())
        {
            if (!snapDfn.isDeleted())
            {
                ret = ret.concatWith(
                    abortSnapshotInTransaction(snapDfn.getResourceName(), snapDfn.getName(), exception)
                );
            }
            else
            {
                if (!updated)
                {
                    updateRequestPrivileged(reqRef);
                    updated = true;
                }
            }
        }
        return ret;
    }

    private Flux<ApiCallRc> abortSnapshotInTransaction(
        ResourceName rscName,
        SnapshotName snapshotName,
        Throwable exception
    )
    {
        Flux<ApiCallRc> retFlux = Flux.empty();

        SnapshotDefinition snapshotDfn = ctrlApiDataLoader.loadSnapshotDfn(rscName, snapshotName, false);
        // might be null when a snapshot (or backup-shipping) is aborted multiple times
        // which can happen as both, sender and receiver try to abort a failed shipment
        if (snapshotDfn != null)
        {
            SnapshotDefinition.Flags flag = exception instanceof CtrlResponseUtils.DelayedApiRcException &&
                isFailNotConnected((CtrlResponseUtils.DelayedApiRcException) exception) ?
                    SnapshotDefinition.Flags.FAILED_DISCONNECT :
                    SnapshotDefinition.Flags.FAILED_DEPLOYMENT;

            enableFlagPrivileged(snapshotDfn, flag);
            // make sure backup shipping does not recognize the failed snap as "in progress"
            disableFlagsPrivileged(snapshotDfn, SnapshotDefinition.Flags.SHIPPING);
            unsetInCreationPrivileged(snapshotDfn);
            ResourceDefinition rscDfn = snapshotDfn.getResourceDefinition();

            for (Snapshot snapshot : getAllSnapshotsPrivileged(snapshotDfn))
            {
                setTakeSnapshotPrivileged(snapshot, false);
            }
            resumeIoPrivileged(rscDfn);

            ctrlTransactionHelper.commit();

            Flux<ApiCallRc> satelliteUpdateResponses = ctrlSatelliteUpdateCaller
                .updateSatellites(
                    new AtomicUpdateSatelliteData().add(rscDfn).add(snapshotDfn),
                    notConnectedCannotAbort()
                )
                .transform(
                    responses -> CtrlResponseUtils.combineResponses(
                        errorReporter,
                        responses,
                        rscName,
                        "Aborted snapshot and resumed IO of {1} on {0}"
                    )
                )
                .onErrorResume(CtrlResponseUtils.DelayedApiRcException.class, ignored -> Flux.empty());

            retFlux = satelliteUpdateResponses
                .concatWith(Flux.error(exception));
        }

        return retFlux;
    }

    private Flux<ApiCallRc> takeSnapshot(CreateMultiSnapRequest reqRef)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Take snapshot",
                lockGuardFactory.create()
                    .read(LockObj.NODES_MAP)
                    .write(LockObj.RSC_DFN_MAP)
                    .buildDeferred(),
                () -> takeSnapshotInTransaction(reqRef),
                MDC.getCopyOfContextMap()
            );
    }

    private Flux<ApiCallRc> takeSnapshotInTransaction(CreateMultiSnapRequest reqRef)
    {
        Flux<ApiCallRc> ret;

        boolean allUpToDate = true;
        List<Snapshot> allSnapshots = new ArrayList<>();
        for (SnapshotDefinition snapDfn : reqRef.getCreatedSnapDfns())
        {
            if (!snapDfn.isDeleted())
            {
                Collection<Snapshot> snapshots = getAllSnapshotsPrivileged(snapDfn);
                allSnapshots.addAll(snapshots);
                boolean snapDfnUpToDate = SatelliteResourceStateDrbdUtils.allResourcesUpToDate(
                    snapshots.stream().map(AbsResource::getNode).collect(Collectors.toSet()),
                    snapDfn.getResourceName(),
                    apiCtx
                );
                if (!snapDfnUpToDate)
                {
                    allUpToDate = false;
                    break;
                }
            }
            else
            {
                allUpToDate = false;
                updateRequestPrivileged(reqRef);
                break;
            }
        }

        if (allUpToDate)
        {
            for (Snapshot snapshot : allSnapshots)
            {
                setTakeSnapshotPrivileged(snapshot, true);
            }
            ctrlTransactionHelper.commit();

            Flux<ApiCallRc> satelliteUpdateResponses = ctrlSatelliteUpdateCaller
                .updateSatellites(reqRef, notConnectedError())
                .transform(
                    responses -> CtrlResponseUtils.combineResponses(
                        errorReporter,
                        responses,
                        reqRef.getJoinedRscNames(),
                        "Took snapshot of {1} on {0}"
                    )
                );

            ret = satelliteUpdateResponses
                .timeout(
                    Duration.ofMinutes(2),
                    abortSnapshot(reqRef, new TimeoutException())
                )
                .concatWith(resumeResource(reqRef));
        }
        else
        {
            for (SnapshotDefinition snapDfn : reqRef.getCreatedSnapDfns())
            {
                errorReporter.logWarning("Not all resources are UpToDate or diskless. Aborting " + snapDfn.toString());
                // no need for isDeleted check here since we already updated the list before. It should not be possible
                // that a SnapshotDefinition is deleted in the current list
                enableFlagPrivileged(snapDfn, SnapshotDefinition.Flags.DELETE);
                unsetInCreationPrivileged(snapDfn);
                ResourceDefinition rscDfn = snapDfn.getResourceDefinition();
                resumeIoPrivileged(rscDfn);
            }

            ctrlTransactionHelper.commit();

            ret = ctrlSatelliteUpdateCaller
                .updateSatellites(reqRef, notConnectedCannotAbort())
                .transform(
                    responses -> CtrlResponseUtils.combineResponses(
                        errorReporter,
                        responses,
                        reqRef.getJoinedRscNames(),
                        "Aborted snapshot of {1} on {0} and resumed IO"
                    )
                );
            for (SnapshotDefinition snapDfn : reqRef.getCreatedSnapDfns())
            {
                // no need for isDeleted check here since we already updated the list before. It should not be possible
                // that a SnapshotDefinition is deleted in the current list
                ret = ret.concatWith(
                    ctrlSnapshotDeleteApiCallHandler.deleteSnapshot(
                        snapDfn.getResourceName(),
                        snapDfn.getName(),
                        null
                    )
                );
            }
            ret = ret.concatWith(
                    Flux.<ApiCallRc>just(
                        ApiCallRcImpl.singleApiCallRc(
                            ApiConsts.FAIL_SNAPSHOT_NOT_UPTODATE,
                        "Cannot take snapshot from non-UpToDate DRBD device. Snapshot creation aborted"
                    ))
                )
                .concatWith(
                    Flux.error(
                        new ApiRcException(
                            ApiCallRcImpl.simpleEntry(
                                ApiConsts.FAIL_SNAPSHOT_NOT_UPTODATE,
                                "Cannot take snapshot from non-UpToDate DRBD device. Snapshot creation aborted"
                            )
                        )
                    )
                )
                .onErrorResume(CtrlResponseUtils.DelayedApiRcException.class, ignored -> Flux.empty());

        }
        return ret;
    }

    private Flux<ApiCallRc> resumeResource(CreateMultiSnapRequest reqRef)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Resume resource",
                lockGuardFactory.create()
                    .read(LockObj.NODES_MAP)
                    .write(LockObj.RSC_DFN_MAP)
                    .buildDeferred(),
                () -> resumeResourceInTransaction(reqRef)
            );
    }

    private Flux<ApiCallRc> resumeResourceInTransaction(CreateMultiSnapRequest reqRef)
    {
        boolean updatedReq = false;
        for (SnapshotDefinition snapDfn : reqRef.getCreatedSnapDfns())
        {
            if (!snapDfn.isDeleted())
            {
                for (Snapshot snapshot : getAllSnapshotsPrivileged(snapDfn))
                {
                    unsetSuspendResourcePrivileged(snapshot);
                    try
                    {
                        snapshot.setCreateTimestamp(apiCtx, new Date());
                    }
                    catch (AccessDeniedException | DatabaseException exc)
                    {
                        errorReporter.reportError(exc);
                    }
                }

                ResourceDefinition rscDfn = snapDfn.getResourceDefinition();
                resumeIoPrivileged(rscDfn);
            }
            else
            {
                if (!updatedReq)
                {
                    updatedReq = true;
                    updateRequestPrivileged(reqRef);
                }
            }
        }

        ctrlTransactionHelper.commit();

        Flux<ApiCallRc> satelliteUpdateResponses = ctrlSatelliteUpdateCaller
            .updateSatellites(reqRef, notConnectedError())
            .transform(
                responses -> CtrlResponseUtils.combineResponses(
                    errorReporter,
                    responses,
                    reqRef.getJoinedRscNames(),
                    "Resumed IO of {1} on {0} after snapshot"
                )
            );

        return satelliteUpdateResponses
            .concatWith(removeInProgressSnapshots(reqRef));
    }

    public Flux<ApiCallRc> removeInProgressSnapshots(CreateMultiSnapRequest reqRef)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Clean up in-progress snapshots",
                lockGuardFactory.create()
                    .read(LockObj.NODES_MAP)
                    .write(LockObj.RSC_DFN_MAP)
                    .buildDeferred(),
                () -> removeInProgressSnapshotsInTransaction(reqRef)
            );
    }

    private Flux<ApiCallRc> removeInProgressSnapshotsInTransaction(CreateMultiSnapRequest reqRef)
    {
        boolean isEbsSnapshot = false;
        boolean updatedRequest = false;
        for (SnapshotDefinition snapDfn : reqRef.getCreatedSnapDfns())
        {
            if (!snapDfn.isDeleted())
            {
                unsetInCreationPrivileged(snapDfn);

                for (Snapshot snapshot : getAllSnapshotsPrivileged(snapDfn))
                {
                    isEbsSnapshot |= isEbsSnapshot(snapshot);
                    setTakeSnapshotPrivileged(snapshot, false);
                }

                enableFlagPrivileged(snapDfn, SnapshotDefinition.Flags.SUCCESSFUL);
            }
            else
            {
                if (!updatedRequest)
                {
                    updatedRequest = true;
                    updateRequestPrivileged(reqRef);
                }
            }
        }

        ctrlTransactionHelper.commit();

        if (isEbsSnapshot)
        {
            pollEbs();
        }

        return ctrlSatelliteUpdateCaller.updateSatellites(reqRef, notConnectedError())
            // ensure that the individual node update fluxes are subscribed to, but ignore responses from cleanup
            .flatMap(Tuple2::getT2).thenMany(Flux.empty());
    }

    private void pollEbs()
    {
        try
        {
            ebsStatusMgr.pollAndWait(EbsStatusManagerService.DFLT_POlL_WAIT);
        }
        catch (InterruptedException exc)
        {
            errorReporter.reportError(exc);
        }
    }

    private boolean isEbsSnapshot(Snapshot snapshotRef)
    {
        boolean ret;
        try
        {
            ret = EbsUtils.isEbs(apiCtx, snapshotRef);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        return ret;
    }

    private void unsetInCreationPrivileged(SnapshotDefinition snapshotDfn)
    {
        try
        {
            snapshotDfn.setInCreation(apiCtx, false);
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

    private void enableFlagPrivileged(SnapshotDefinition snapshotDfn, SnapshotDefinition.Flags flag)
    {
        try
        {
            snapshotDfn.getFlags().enableFlags(apiCtx, flag);
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

    private void disableFlagsPrivileged(SnapshotDefinition snapshotDfn, SnapshotDefinition.Flags... flags)
    {
        try
        {
            snapshotDfn.getFlags().disableFlags(apiCtx, flags);
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

    @Deprecated
    private void unsetSuspendResourcePrivileged(Snapshot snapshot)
    {
        try
        {
            snapshot.setSuspendResource(apiCtx, false);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
    }

    private void resumeIoPrivileged(ResourceDefinition rscDfn)
    {
        try
        {
            Iterator<Resource> rscIt = rscDfn.iterateResource(apiCtx);
            while (rscIt.hasNext())
            {
                Resource rsc = rscIt.next();
                SuspendLayerUtils.resumeIo(apiCtx, rsc);
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        catch (DatabaseException dbExc)
        {
            throw new ApiDatabaseException(dbExc);
        }
    }

    private Collection<Snapshot> getAllSnapshotsPrivileged(SnapshotDefinition snapshotDfn)
    {
        Collection<Snapshot> allSnapshots;
        try
        {
            allSnapshots = snapshotDfn.getAllSnapshots(apiCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        return allSnapshots;
    }

    private void setTakeSnapshotPrivileged(Snapshot snapshot, boolean takeSnapshot)
    {
        try
        {
            snapshot.setTakeSnapshot(apiCtx, takeSnapshot);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
    }

    private CreateMultiSnapRequest createSimpleSnapshotRequestPrivileged(SnapshotDefinition snapDfnRef)
    {
        try
        {
            return new CreateMultiSnapRequest(apiCtx, snapDfnRef);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private void updateRequestPrivileged(CreateMultiSnapRequest reqRef)
    {
        try
        {
            reqRef.update(apiCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
    }
    private static boolean isFailNotConnected(CtrlResponseUtils.DelayedApiRcException exception)
    {
        return exception.getErrors().stream().flatMap(
            apiRcException -> apiRcException.getApiCallRc().stream()
        ).anyMatch(rcEntry -> rcEntry.getReturnCode() == ApiConsts.FAIL_NOT_CONNECTED);
    }

    private static CtrlSatelliteUpdateCaller.NotConnectedHandler notConnectedCannotAbort()
    {
        return nodeName -> Flux.error(
            new ApiRcException(
                ApiCallRcImpl.entryBuilder(
                    ApiConsts.WARN_NOT_CONNECTED,
                    "Unable to abort snapshot process on disconnected satellite '" + nodeName + "'"
                ).setDetails(
                    "IO may be suspended until the connection to the satellite is re-established"
                ).build()
            )
        );
    }
}
