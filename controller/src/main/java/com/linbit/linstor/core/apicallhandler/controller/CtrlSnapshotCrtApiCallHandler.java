package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.TimeoutException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSuccessUtils;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotApiCallHandler.getSnapshotDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotApiCallHandler.makeSnapshotContext;
import static com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller.notConnectedError;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

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
        CtrlSnapshotDeleteApiCallHandler ctrlSnapshotDeleteApiCallHandlerRef
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
    }

    /**
     * Create a snapshot of a resource.
     * <p>
     * Snapshots are created in a multi-stage process:
     * <ol>
     *     <li>Add the snapshot objects (definition and instances), marked with the suspend flag</li>
     *     <li>When all resources are suspended, send out a snapshot request</li>
     *     <li>When all snapshots have been created, mark the resource as resuming by removing the suspend flag</li>
     *     <li>When all resources have been resumed, remove the in-progress snapshots</li>
     * </ol>
     */
    public Flux<ApiCallRc> createSnapshot(
        List<String> nodeNameStrs,
        String rscNameStr,
        String snapshotNameStr
    )
    {
        ResponseContext context = makeSnapshotContext(
            ApiOperation.makeCreateOperation(),
            nodeNameStrs,
            rscNameStr,
            snapshotNameStr
        );

        return scopeRunner
            .fluxInTransactionalScope(
                "Create snapshot",
                lockGuardFactory.create()
                    .read(LockObj.NODES_MAP)
                    .write(LockObj.RSC_DFN_MAP)
                    .buildDeferred(),
                () -> createSnapshotInTransaction(nodeNameStrs, rscNameStr, snapshotNameStr)
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> createSnapshotInTransaction(
        List<String> nodeNameStrs,
        String rscNameStr,
        String snapshotNameStr
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();

        SnapshotDefinition snapshotDfn = ctrlSnapshotCrtHelper.createSnapshots(
            nodeNameStrs,
            rscNameStr,
            snapshotNameStr,
            responses
        );

        ctrlTransactionHelper.commit();

        responses.addEntry(
            ApiSuccessUtils.defaultRegisteredEntry(
                snapshotDfn.getUuid(),
                getSnapshotDescriptionInline(nodeNameStrs, rscNameStr, snapshotNameStr)
            )
        );
        return Flux.<ApiCallRc>just(responses)
            .concatWith(postCreateSnapshot(snapshotDfn));
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
            } while (snapDfn != null);

            ApiCallRcImpl responses = new ApiCallRcImpl();

            snapDfn = ctrlSnapshotCrtHelper.createSnapshots(
                nodeNameStrs,
                rscNameStr,
                autoSnapshotName,
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
                .concatWith(postCreateSnapshot(snapDfn))
                .concatWith(ctrlSnapshotDeleteApiCallHandler.cleanupOldAutoSnapshots(rscDfn));
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
                if (!node.getPeer(apiCtx).isConnected())
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
        String snapPrefix = rscDfnProps.getPropWithDefault(
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

        return String.format(snapPrefix + "%05d", id);
    }

    /**
     * After Snapshots with their SnapshotDefinition were created, this method now sends the update
     * to the corresponding satellites and also takes care of resuming-io in the end.
     *
     * @param snapshotDfn
     *
     * @return
     */
    Flux<ApiCallRc> postCreateSnapshot(SnapshotDefinition snapshotDfn)
    {
        ResourceDefinition rscDfn = snapshotDfn.getResourceDefinition();

        ResourceName rscName = rscDfn.getName();
        SnapshotName snapshotName = snapshotDfn.getName();


        Flux<ApiCallRc> satelliteUpdateResponses =
            ctrlSatelliteUpdateCaller.updateSatellites(snapshotDfn, notConnectedError())
                .concatWith(ctrlSatelliteUpdateCaller.updateSatellites(rscDfn, notConnectedError(), Flux.empty()))
                .transform(
                    updateResponses -> CtrlResponseUtils.combineResponses(
                        updateResponses,
                        rscName,
                        "Suspended IO of {1} on {0} for snapshot"
            )
        );

        return satelliteUpdateResponses
            .concatWith(takeSnapshot(rscName, snapshotName))
            .onErrorResume(exception -> abortSnapshot(rscName, snapshotName, exception))
            .onErrorResume(CtrlResponseUtils.DelayedApiRcException.class, ignored -> Flux.empty());
    }

    private Flux<ApiCallRc> abortSnapshot(
        ResourceName rscName,
        SnapshotName snapshotName,
        Throwable exception
    )
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Abort taking snapshot",
                lockGuardFactory.create()
                    .read(LockObj.NODES_MAP)
                    .write(LockObj.RSC_DFN_MAP)
                    .buildDeferred(),
                () -> abortSnapshotInTransaction(rscName, snapshotName, exception)
            );
    }

    private Flux<ApiCallRc> abortSnapshotInTransaction(
        ResourceName rscName,
        SnapshotName snapshotName,
        Throwable exception
    )
    {
        SnapshotDefinition snapshotDfn = ctrlApiDataLoader.loadSnapshotDfn(rscName, snapshotName, true);

        SnapshotDefinition.Flags flag = exception instanceof CtrlResponseUtils.DelayedApiRcException &&
            isFailNotConnected((CtrlResponseUtils.DelayedApiRcException) exception) ?
                SnapshotDefinition.Flags.FAILED_DISCONNECT : SnapshotDefinition.Flags.FAILED_DEPLOYMENT;

        enableFlagPrivileged(snapshotDfn, flag);
        unsetInCreationPrivileged(snapshotDfn);
        ResourceDefinition rscDfn = snapshotDfn.getResourceDefinition();
        resumeIoPrivileged(rscDfn);

        ctrlTransactionHelper.commit();

        Flux<ApiCallRc> satelliteUpdateResponses =
            ctrlSatelliteUpdateCaller.updateSatellites(snapshotDfn, notConnectedCannotAbort())
                .transform(
                    responses -> CtrlResponseUtils.combineResponses(
                        responses,
                        rscName,
                        "Aborted snapshot of {1} on {0}"
                    )
                )
                .concatWith(
                    ctrlSatelliteUpdateCaller.updateSatellites(rscDfn, Flux.empty())
                        .transform(
                            responses -> CtrlResponseUtils.combineResponses(
                                responses,
                                rscName,
                                "Resumed IO of {1} on {0} after failed snapshot"
                            )
                        )
                    )
                .onErrorResume(CtrlResponseUtils.DelayedApiRcException.class, ignored -> Flux.empty());

        return satelliteUpdateResponses
            .concatWith(Flux.error(exception));
    }

    private Flux<ApiCallRc> takeSnapshot(ResourceName rscName, SnapshotName snapshotName)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Take snapshot",
                lockGuardFactory.create()
                    .read(LockObj.NODES_MAP)
                    .write(LockObj.RSC_DFN_MAP)
                    .buildDeferred(),
                () -> takeSnapshotInTransaction(rscName, snapshotName)
            );
    }

    private Flux<ApiCallRc> takeSnapshotInTransaction(ResourceName rscName, SnapshotName snapshotName)
    {
        SnapshotDefinition snapshotDfn = ctrlApiDataLoader.loadSnapshotDfn(rscName, snapshotName, true);

        for (Snapshot snapshot : getAllSnapshotsPrivileged(snapshotDfn))
        {
            setTakeSnapshotPrivileged(snapshot, true);
        }

        ctrlTransactionHelper.commit();

        Flux<ApiCallRc> satelliteUpdateResponses = ctrlSatelliteUpdateCaller
            .updateSatellites(snapshotDfn, notConnectedError())
            .transform(
                responses -> CtrlResponseUtils.combineResponses(
                    responses,
                    rscName,
                    "Took snapshot of {1} on {0}"
                )
            );

        return satelliteUpdateResponses
            .timeout(
                Duration.ofMinutes(2),
                abortSnapshot(rscName, snapshotName, new TimeoutException())
            )
            .concatWith(resumeResource(rscName, snapshotName));
    }

    private Flux<ApiCallRc> resumeResource(ResourceName rscName, SnapshotName snapshotName)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Resume resource",
                lockGuardFactory.create()
                    .read(LockObj.NODES_MAP)
                    .write(LockObj.RSC_DFN_MAP)
                    .buildDeferred(),
                () -> resumeResourceInTransaction(rscName, snapshotName)
            );
    }

    private Flux<ApiCallRc> resumeResourceInTransaction(ResourceName rscName, SnapshotName snapshotName)
    {
        SnapshotDefinition snapshotDfn = ctrlApiDataLoader.loadSnapshotDfn(rscName, snapshotName, true);

        for (Snapshot snapshot : getAllSnapshotsPrivileged(snapshotDfn))
        {
            unsetSuspendResourcePrivileged(snapshot);
        }

        ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(rscName, true);
        resumeIoPrivileged(rscDfn);

        ctrlTransactionHelper.commit();

        Flux<ApiCallRc> satelliteUpdateResponses = ctrlSatelliteUpdateCaller
            .updateSatellites(snapshotDfn, notConnectedError())
            .concatWith(ctrlSatelliteUpdateCaller.updateSatellites(rscDfn, notConnectedError(), Flux.empty()))
            .transform(
                responses -> CtrlResponseUtils.combineResponses(
                    responses,
                    rscName,
                    "Resumed IO of {1} on {0} after snapshot"
                )
            );

        return satelliteUpdateResponses
            .concatWith(removeInProgressSnapshots(rscName, snapshotName));
    }

    private Flux<ApiCallRc> removeInProgressSnapshots(ResourceName rscName, SnapshotName snapshotName)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Clean up in-progress snapshots",
                lockGuardFactory.create()
                    .read(LockObj.NODES_MAP)
                    .write(LockObj.RSC_DFN_MAP)
                    .buildDeferred(),
                () -> removeInProgressSnapshotsInTransaction(rscName, snapshotName)
            );
    }

    private Flux<ApiCallRc> removeInProgressSnapshotsInTransaction(ResourceName rscName, SnapshotName snapshotName)
    {
        SnapshotDefinition snapshotDfn = ctrlApiDataLoader.loadSnapshotDfn(rscName, snapshotName, true);

        unsetInCreationPrivileged(snapshotDfn);

        for (Snapshot snapshot : getAllSnapshotsPrivileged(snapshotDfn))
        {
            setTakeSnapshotPrivileged(snapshot, false);
        }

        enableFlagPrivileged(snapshotDfn, SnapshotDefinition.Flags.SUCCESSFUL);

        ctrlTransactionHelper.commit();

        return ctrlSatelliteUpdateCaller.updateSatellites(snapshotDfn, notConnectedError())
            // ensure that the individual node update fluxes are subscribed to, but ignore responses from cleanup
            .flatMap(Tuple2::getT2).thenMany(Flux.empty());
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
                rsc.getLayerData(apiCtx).setSuspendIo(false);
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

    private static boolean isFailNotConnected(CtrlResponseUtils.DelayedApiRcException exception)
    {
        return exception.getErrors().stream().flatMap(
            apiRcException -> apiRcException.getApiCallRc().getEntries().stream()
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
