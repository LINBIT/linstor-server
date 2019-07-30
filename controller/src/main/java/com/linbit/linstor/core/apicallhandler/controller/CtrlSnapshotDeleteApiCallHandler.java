package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
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
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotDefinitionData;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotApiCallHandler.getSnapshotDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotApiCallHandler.getSnapshotDfnDescription;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotApiCallHandler.getSnapshotDfnDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotApiCallHandler.makeSnapshotContext;
import static com.linbit.utils.StringUtils.firstLetterCaps;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import reactor.core.publisher.Flux;

@Singleton
public class CtrlSnapshotDeleteApiCallHandler implements CtrlSatelliteConnectionListener
{
    private final AccessContext apiCtx;
    private final ScopeRunner scopeRunner;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final ResponseConverter responseConverter;
    private final LockGuardFactory lockGuardFactory;
    private final Provider<AccessContext> peerAccCtx;

    @Inject
    public CtrlSnapshotDeleteApiCallHandler(
        @ApiContext AccessContext apiCtxRef,
        ScopeRunner scopeRunnerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        ResponseConverter responseConverterRef,
        LockGuardFactory lockguardFactoryRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef
    )
    {
        apiCtx = apiCtxRef;
        scopeRunner = scopeRunnerRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        responseConverter = responseConverterRef;
        lockGuardFactory = lockguardFactoryRef;
        peerAccCtx = peerAccCtxRef;
    }

    @Override
    public Collection<Flux<ApiCallRc>> resourceDefinitionConnected(ResourceDefinition rscDfn)
        throws AccessDeniedException
    {
        List<Flux<ApiCallRc>> fluxes = new ArrayList<>();

        for (SnapshotDefinition snapshotDfn : rscDfn.getSnapshotDfns(apiCtx))
        {
            if (snapshotDfn.getFlags().isSet(apiCtx, SnapshotDefinition.SnapshotDfnFlags.DELETE))
            {
                fluxes.add(deleteSnapshotsOnNodes(rscDfn.getName(), snapshotDfn.getName()));
            }
        }

        return fluxes;
    }

    public Flux<ApiCallRc> deleteSnapshot(String rscNameStr, String snapshotNameStr)
    {
        ResponseContext context = makeSnapshotContext(
            ApiOperation.makeDeleteOperation(),
            Collections.emptyList(),
            rscNameStr,
            snapshotNameStr
        );

        return scopeRunner
            .fluxInTransactionalScope(
                "Delete snapshot",
                lockGuardFactory.create()
                    .read(LockObj.NODES_MAP)
                    .write(LockObj.RSC_DFN_MAP)
                    .buildDeferred(),
                () -> deleteSnapshotInTransaction(rscNameStr, snapshotNameStr)
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> deleteSnapshotInTransaction(String rscNameStr, String snapshotNameStr)
    {
        SnapshotDefinition snapshotDfn = ctrlApiDataLoader.loadSnapshotDfn(rscNameStr, snapshotNameStr, false);

        if (snapshotDfn == null)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.WARN_NOT_FOUND,
                    getSnapshotDfnDescription(snapshotNameStr) + " not found."
            ));
        }

        ResourceName rscName = snapshotDfn.getResourceName();
        SnapshotName snapshotName = snapshotDfn.getName();

        markSnapshotDfnDeleted(snapshotDfn);

        for (Snapshot snapshot : getAllSnapshots(snapshotDfn))
        {
            markSnapshotDeleted(snapshot);
        }

        ctrlTransactionHelper.commit();

        ApiCallRc responses = ApiCallRcImpl.singletonApiCallRc(ApiCallRcImpl.simpleEntry(
            ApiConsts.DELETED,
            firstLetterCaps(getSnapshotDfnDescriptionInline(rscName, snapshotName)) + " marked for deletion."
        ));

        return Flux
            .just(responses)
            .concatWith(deleteSnapshotsOnNodes(rscName, snapshotName));
    }

    // Restart from here when connection established and DELETE flag set
    private Flux<ApiCallRc> deleteSnapshotsOnNodes(ResourceName rscName, SnapshotName snapshotName)
    {
        return scopeRunner
            .fluxInTransactionlessScope(
                "Delete snapshots on nodes",
                lockGuardFactory.buildDeferred(LockType.READ, LockObj.NODES_MAP, LockObj.RSC_DFN_MAP),
                () -> deleteSnapshotsOnNodesInScope(rscName, snapshotName)
            );
    }

    private Flux<ApiCallRc> deleteSnapshotsOnNodesInScope(ResourceName rscName, SnapshotName snapshotName)
    {
        SnapshotDefinitionData snapshotDfn = ctrlApiDataLoader.loadSnapshotDfn(rscName, snapshotName, false);

        Flux<ApiCallRc> flux;
        if (snapshotDfn == null)
        {
            flux = Flux.empty();
        }
        else
        {
            Flux<ApiCallRc> satelliteUpdateResponses =
                ctrlSatelliteUpdateCaller.updateSatellites(snapshotDfn, CtrlSatelliteUpdateCaller.notConnectedWarn())
                    .transform(responses -> CtrlResponseUtils.combineResponses(
                        responses,
                        rscName,
                        "Deleted snapshot ''" + snapshotName + "'' of {1} on {0}"
                    ));

            flux = satelliteUpdateResponses
                .concatWith(deleteData(rscName, snapshotName))
                .onErrorResume(CtrlResponseUtils.DelayedApiRcException.class, ignored -> Flux.empty());
        }
        return flux;
    }

    private Flux<ApiCallRc> deleteData(ResourceName rscName, SnapshotName snapshotName)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Delete snapshot data",
                lockGuardFactory.create()
                    .read(LockObj.NODES_MAP)
                    .write(LockObj.RSC_DFN_MAP)
                    .buildDeferred(),
                () -> deleteDataInTransaction(rscName, snapshotName)
            );
    }

    private Flux<ApiCallRc> deleteDataInTransaction(ResourceName rscName, SnapshotName snapshotName)
    {
        SnapshotDefinitionData snapshotDfn = ctrlApiDataLoader.loadSnapshotDfn(rscName, snapshotName, false);

        Flux<ApiCallRc> flux;
        if (snapshotDfn == null)
        {
            flux = Flux.empty();
        }
        else
        {
            UUID uuid = snapshotDfn.getUuid();

            for (Snapshot snapshot : new ArrayList<>(getAllSnapshotsPrivileged(snapshotDfn)))
            {
                deleteSnapshotPrivileged(snapshot);
            }
            deleteSnapshotDfnPrivileged(snapshotDfn);

            ctrlTransactionHelper.commit();

            ApiCallRcImpl responses = new ApiCallRcImpl();

            responses.addEntry(ApiSuccessUtils.defaultDeletedEntry(
                uuid, getSnapshotDfnDescriptionInline(rscName, snapshotName)
            ));

            flux = Flux.just(responses);
        }
        return flux;
    }

    private void markSnapshotDfnDeleted(SnapshotDefinition snapshotDfn)
    {
        try
        {
            snapshotDfn.markDeleted(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "mark " + getSnapshotDfnDescriptionInline(snapshotDfn) + " as deleted",
                ApiConsts.FAIL_ACC_DENIED_SNAPSHOT_DFN
            );
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
    }

    private void markSnapshotDeleted(Snapshot snapshot)
    {
        try
        {
            snapshot.markDeleted(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "mark " + getSnapshotDescriptionInline(snapshot) + " as deleted",
                ApiConsts.FAIL_ACC_DENIED_SNAPSHOT
            );
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
    }

    private Collection<Snapshot> getAllSnapshots(SnapshotDefinition snapshotDfn)
    {
        Collection<Snapshot> allSnapshots;
        try
        {
            allSnapshots = snapshotDfn.getAllSnapshots(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "get snapshots of " + getSnapshotDfnDescriptionInline(snapshotDfn),
                ApiConsts.FAIL_ACC_DENIED_SNAPSHOT_DFN
            );
        }
        return allSnapshots;
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

    private void deleteSnapshotDfnPrivileged(SnapshotDefinitionData snapshotDfn)
    {
        try
        {
            snapshotDfn.delete(apiCtx);
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

    private void deleteSnapshotPrivileged(Snapshot snapshot)
    {
        try
        {
            snapshot.delete(apiCtx);
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
}
