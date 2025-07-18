package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.backupshipping.BackupShippingUtils;
import com.linbit.linstor.core.BackupInfoManager;
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
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;
import com.linbit.utils.StringUtils;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotApiCallHandler.getSnapshotDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotApiCallHandler.getSnapshotDfnDescription;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotApiCallHandler.getSnapshotDfnDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotApiCallHandler.makeSnapshotContext;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;
import java.util.UUID;
import java.util.regex.Pattern;

import org.slf4j.MDC;
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
    private final CtrlPropsHelper propsHelper;
    private final ErrorReporter errorReporter;
    private final BackupInfoManager backupInfoMgr;

    @Inject
    public CtrlSnapshotDeleteApiCallHandler(
        @ApiContext AccessContext apiCtxRef,
        ScopeRunner scopeRunnerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        ResponseConverter responseConverterRef,
        LockGuardFactory lockguardFactoryRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        CtrlPropsHelper propsHelperRef,
        ErrorReporter errorReporterRef,
        BackupInfoManager backupInfoMgrRef
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
        propsHelper = propsHelperRef;
        errorReporter = errorReporterRef;
        backupInfoMgr = backupInfoMgrRef;
    }

    @Override
    public Collection<Flux<ApiCallRc>> resourceDefinitionConnected(ResourceDefinition rscDfn, ResponseContext context)
        throws AccessDeniedException
    {
        List<Flux<ApiCallRc>> fluxes = new ArrayList<>();

        for (SnapshotDefinition snapshotDfn : rscDfn.getSnapshotDfns(apiCtx))
        {
            if (snapshotDfn.getFlags().isSet(apiCtx, SnapshotDefinition.Flags.DELETE))
            {
                fluxes.add(deleteSnapshotsOnNodes(rscDfn.getName(), snapshotDfn.getName()));
            }
        }

        return fluxes;
    }

    /**
     * deletes a snapshot
     * this should be called directly by the REST-class and therefore needs its own exception handling
     *
     * @param rscName
     * @param snapshotName
     * @param nodeNamesStrListRef
     *
     * @return deletion-flux
     */
    public Flux<ApiCallRc> deleteSnapshot(
        String rscNameStr,
        String snapshotNameStr,
        @Nullable List<String> nodeNamesStrListRef
    )
    {
        ResponseContext context = makeSnapshotContext(
            ApiOperation.makeDeleteOperation(),
            Collections.emptyList(),
            rscNameStr,
            snapshotNameStr
        );
        Flux<ApiCallRc> ret;
        try
        {
            ret = deleteSnapshot(
                LinstorParsingUtils.asRscName(rscNameStr),
                LinstorParsingUtils.asSnapshotName(snapshotNameStr),
                nodeNamesStrListRef
            )
                .transform(responses -> responseConverter.reportingExceptions(context, responses));
        }
        catch (ApiRcException exc)
        {
            ret = Flux.error(exc);
        }
        return ret;
    }

    /**
     * deletes a snapshot
     * this should be called only internally and therefore leaves the exception handling to its callers
     *
     * @param rscName
     * @param snapshotName
     * @param nodeNamesStrListRef
     *
     * @return deletion-flux or error-flux in case of exception
     */
    public Flux<ApiCallRc> deleteSnapshot(
        ResourceName rscName,
        SnapshotName snapshotName,
        List<String> nodeNamesStrListRef
    )
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Delete snapshot",
                lockGuardFactory.create()
                    .read(LockObj.NODES_MAP)
                    .write(LockObj.RSC_DFN_MAP)
                    .buildDeferred(),
                () -> deleteSnapshotInTransaction(rscName, snapshotName, nodeNamesStrListRef)
            );
    }

    private Flux<ApiCallRc> deleteSnapshotInTransaction(
        ResourceName rscNameRef,
        SnapshotName snapshotNameRef,
        @Nullable List<String> nodeNamesStrListRef
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        SnapshotDefinition snapshotDfn = ctrlApiDataLoader.loadSnapshotDfn(
            rscNameRef,
            snapshotNameRef,
            false
        );

        if (snapshotDfn == null)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.WARN_NOT_FOUND,
                getSnapshotDfnDescription(rscNameRef, snapshotNameRef) + " not found."
            ));
        }
        ResourceName rscName = snapshotDfn.getResourceName();
        SnapshotName snapshotName = snapshotDfn.getName();
        if (backupInfoMgr.restoreContainsRscDfn(snapshotDfn.getResourceDefinition()))
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_IN_USE,
                    getSnapshotDfnDescription(rscName, snapshotName) + " is currently being restored " +
                        "from a backup. " +
                        "Please wait until the restore is finished"
                )
            );
        }

        if (isBackupShippingInProgress(snapshotDfn))
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_IN_USE,
                    getSnapshotDfnDescription(rscName, snapshotName) + " is currently being shipped " +
                        "as a backup. " +
                        "Please wait until the shipping is finished or use backup abort --create"
                )
            );
        }
        ensureSnapshotNotQueued(snapshotDfn);

        if (nodeNamesStrListRef == null || nodeNamesStrListRef.isEmpty())
        {
            markSnapshotDfnDeleted(snapshotDfn);
            for (Snapshot snapshot : getAllSnapshots(snapshotDfn))
            {
                responses.addEntry(
                    "Marked snapshot for deletion " + getSnapshotDescriptionInline(snapshot),
                    ApiConsts.DELETED
                );
                markSnapshotDeleted(snapshot);
            }
        }
        else
        {
            // original list could be the result of a stream.collect. However, we will want to remove from this list
            List<String> nodeNamesStrCopy = new ArrayList<>(nodeNamesStrListRef);

            List<String> nodeNamesToDelete = new ArrayList<>();
            boolean hasSnapshotsNotBeingDeleted = false;
            for (Snapshot snapshot : getAllSnapshots(snapshotDfn))
            {
                String foundNodeNameStr = null;
                for (String nodeNameStr : nodeNamesStrCopy)
                {
                    if (nodeNameStr.equalsIgnoreCase(snapshot.getNodeName().displayValue))
                    {
                        foundNodeNameStr = nodeNameStr;
                        nodeNamesToDelete.add(snapshot.getNodeName().displayValue);
                        markSnapshotDeleted(snapshot);
                    }
                }
                if (foundNodeNameStr != null)
                {
                    nodeNamesStrCopy.remove(foundNodeNameStr);
                }
                hasSnapshotsNotBeingDeleted |= !isFlagSet(snapshot, Snapshot.Flags.DELETE);
            }
            responses.addEntry(
                "Marked snapshot for deletion " + getSnapshotDescriptionInline(
                    nodeNamesToDelete,
                    snapshotDfn.getResourceName().displayValue,
                    snapshotDfn.getName().displayValue
                ),
                ApiConsts.DELETED
            );
            if (!nodeNamesStrCopy.isEmpty())
            {
                responses.addEntry(
                    getSnapshotDfnDescription(rscName, snapshotName) +
                        " was not found on given nodes: " + StringUtils.join(nodeNamesStrCopy, ", "),
                    ApiConsts.WARN_NOT_FOUND
                );
            }
            if (!hasSnapshotsNotBeingDeleted)
            {
                markSnapshotDfnDeleted(snapshotDfn);
            }
        }

        ctrlTransactionHelper.commit();

        return Flux.<ApiCallRc>just(responses)
            .concatWith(deleteSnapshotsOnNodes(rscName, snapshotName));
    }

    private void ensureSnapshotNotQueued(SnapshotDefinition snapDfn)
    {
        if (backupInfoMgr.isSnapshotQueued(snapDfn))
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_IN_USE,
                    getSnapshotDfnDescription(
                        snapDfn.getResourceName(),
                        snapDfn.getName()
                    ) +
                        " is currently being queued for backup shipping. " +
                        "Please wait until the shipping is finished or use backup abort --create"
                )
            );
        }
    }

    private boolean isBackupShippingInProgress(SnapshotDefinition snapshotDfnRef)
    {
        boolean shipping = false;
        try
        {
            shipping = BackupShippingUtils.isAnyShippingInProgress(snapshotDfnRef, peerAccCtx.get());
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "checking if SnapshotDefinition is in progress",
                ApiConsts.FAIL_ACC_DENIED_SNAP_DFN
            );
        }
        return shipping;
    }

    // Restart from here when connection established and DELETE flag set
    public Flux<ApiCallRc> deleteSnapshotsOnNodes(ResourceName rscName, SnapshotName snapshotName)
    {
        return scopeRunner
            .fluxInTransactionlessScope(
                "Delete snapshots on nodes",
                lockGuardFactory.buildDeferred(LockType.READ, LockObj.NODES_MAP, LockObj.RSC_DFN_MAP),
                () -> deleteSnapshotsOnNodesInScope(rscName, snapshotName),
                MDC.getCopyOfContextMap()
            );
    }

    private Flux<ApiCallRc> deleteSnapshotsOnNodesInScope(ResourceName rscName, SnapshotName snapshotName)
    {
        SnapshotDefinition snapshotDfn = ctrlApiDataLoader.loadSnapshotDfn(rscName, snapshotName, false);

        Flux<ApiCallRc> flux;
        if (snapshotDfn == null)
        {
            flux = Flux.empty();
        }
        else
        {
            List<NodeName> nodeNamesToDelete = new ArrayList<>();
            for (Snapshot snapshot : getAllSnapshotsPrivileged(snapshotDfn))
            {
                if (isFlagSet(snapshot, Snapshot.Flags.DELETE))
                {
                    nodeNamesToDelete.add(snapshot.getNodeName());
                }
            }

            Flux<ApiCallRc> satelliteUpdateResponses =
                ctrlSatelliteUpdateCaller.updateSatellites(snapshotDfn, CtrlSatelliteUpdateCaller.notConnectedWarn())
                    .transform(responses -> CtrlResponseUtils.combineResponses(
                        errorReporter,
                        responses,
                        rscName,
                        nodeNamesToDelete,
                        "Deleted snapshot ''" + snapshotName + "'' of {1} on {0}",
                        "Updated snapshot ''" + snapshotName + "'' of {1} on {0}"
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
        SnapshotDefinition snapshotDfn = ctrlApiDataLoader.loadSnapshotDfn(rscName, snapshotName, false);

        Flux<ApiCallRc> flux;
        if (snapshotDfn == null)
        {
            flux = Flux.empty();
        }
        else
        {
            ApiCallRcImpl responses = new ApiCallRcImpl();

            List<String> deletedSnapOnNodeNames = new ArrayList<>();
            for (Snapshot snapshot : new ArrayList<>(getAllSnapshotsPrivileged(snapshotDfn)))
            {
                if (isFlagSet(snapshot, Snapshot.Flags.DELETE))
                {
                    deletedSnapOnNodeNames.add(snapshot.getNodeName().displayValue);
                    deleteSnapshotPrivileged(snapshot);
                }
            }
            if (!deletedSnapOnNodeNames.isEmpty())
            {
                responses.addEntry(
                    ApiSuccessUtils.defaultDeletedEntry(
                        null,
                        getSnapshotDescriptionInline(
                            deletedSnapOnNodeNames,
                            rscName.displayValue,
                            snapshotName.displayValue
                        )
                    )
                );
            }
            if (isFlagSet(snapshotDfn, SnapshotDefinition.Flags.DELETE))
            {
                UUID uuid = snapshotDfn.getUuid();
                deleteSnapshotDfnPrivileged(snapshotDfn);
                responses.addEntry(
                    ApiSuccessUtils.defaultDeletedEntry(
                        uuid,
                        getSnapshotDfnDescriptionInline(rscName, snapshotName)
                    )
                );
            }

            ctrlTransactionHelper.commit();

            flux = Flux.just(responses);
        }
        return flux;
    }

    public Flux<ApiCallRc> cleanupOldAutoSnapshots(ResourceDefinition rscDfnRef)
    {
        return scopeRunner.fluxInTransactionalScope(
            "Clean up old auto-snapshots",
            lockGuardFactory.create().read(LockObj.NODES_MAP).write(LockObj.RSC_DFN_MAP).buildDeferred(),
            () -> cleanupOldSnapshotsInTransaction(
                rscDfnRef,
                ApiConsts.NAMESPC_AUTO_SNAPSHOT,
                ApiConsts.KEY_KEEP,
                ApiConsts.DFLT_AUTO_SNAPSHOT_KEEP,
                ApiConsts.KEY_AUTO_SNAPSHOT_PREFIX,
                InternalApiConsts.DEFAULT_AUTO_SNAPSHOT_PREFIX,
                SnapshotDefinition.Flags.AUTO_SNAPSHOT
            )
        );
    }


    private Flux<ApiCallRc> cleanupOldSnapshotsInTransaction(
        ResourceDefinition rscDfnRef,
        String rscDfnPropNameSpc,
        String rscDfnPropKeepKey,
        String rscDfnPropKeepDfltValue,
        String rscDfnPropPrefixKey,
        String rscDfnPropPrefixDfltValue,
        SnapshotDefinition.Flags snapDfnFilterFlag
    )
    {
        Flux<ApiCallRc> flux = Flux.empty();
        try
        {
            PriorityProps prioProps = new PriorityProps(
                propsHelper.getProps(rscDfnRef),
                propsHelper.getProps(rscDfnRef.getResourceGroup()),
                propsHelper.getStltPropsForView()
            );
            String keepStr = prioProps.getProp(
                rscDfnPropKeepKey,
                rscDfnPropNameSpc,
                rscDfnPropKeepDfltValue
            );
            long keep;
            try
            {
                keep = Long.parseLong(keepStr);

                if (keep > 0)
                {
                    String snapPrefix = prioProps.getProp(
                        rscDfnPropPrefixKey,
                        rscDfnPropNameSpc,
                        rscDfnPropPrefixDfltValue
                    );

                    Pattern autoPattern = Pattern.compile("^" + snapPrefix + "[0-9]+$");
                    /*
                     * automatically sorts snapDfns by name, which should make the snapshot with the lowest
                     * ID first
                     */
                    TreeSet<SnapshotDefinition> sortedSnapDfnSet = new TreeSet<>();
                    for (SnapshotDefinition snapDfn : rscDfnRef.getSnapshotDfns(apiCtx))
                    {
                        if (
                            snapDfn.getFlags().isSet(apiCtx, snapDfnFilterFlag) &&
                                autoPattern.matcher(snapDfn.getName().displayValue).matches()
                        )
                        {
                            sortedSnapDfnSet.add(snapDfn);
                        }
                    }

                    ApiCallRcImpl responses = new ApiCallRcImpl();
                    while (keep < sortedSnapDfnSet.size())
                    {
                        SnapshotDefinition autoSnapToDelete = sortedSnapDfnSet.first();
                        sortedSnapDfnSet.remove(autoSnapToDelete);

                        flux = flux.concatWith(
                            deleteSnapshot(
                                autoSnapToDelete.getResourceName(),
                                autoSnapToDelete.getName(),
                                null
                            )
                        );
                        responses.addEntries(
                            ApiCallRcImpl.singleApiCallRc(
                                ApiConsts.DELETED,
                                "AutoSnapshot cleanup: deleting snapshot " + autoSnapToDelete.getName().displayValue
                            )
                        );
                        errorReporter.logDebug(
                            "AutoSnapshot.cleanup: deleting %s",
                            autoSnapToDelete.getName().displayValue
                        );
                    }
                    flux = Flux.<ApiCallRc>just(responses)
                        .concatWith(flux);
                }
                else
                {
                    errorReporter.logDebug("AutoSnapshot/Keep is configured to %d. Keeping all snapshots", keep);
                }
            }
            catch (NumberFormatException nfe)
            {
                errorReporter.reportError(
                    nfe,
                    apiCtx,
                    null,
                    "Invalid value for property " + rscDfnPropNameSpc + "/" + rscDfnPropKeepKey
                );
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        ctrlTransactionHelper.commit();

        return flux;
    }

    private boolean isFlagSet(Snapshot snapshotRef, Snapshot.Flags... flags)
    {
        boolean ret;
        try
        {
            ret = snapshotRef.getFlags().isSet(peerAccCtx.get(), flags);
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "access snapshot " + getSnapshotDescriptionInline(snapshotRef),
                ApiConsts.FAIL_ACC_DENIED_SNAPSHOT
            );
        }
        return ret;
    }

    private boolean isFlagSet(SnapshotDefinition snapDfnRef, SnapshotDefinition.Flags... flags)
    {
        boolean ret;
        try
        {
            ret = snapDfnRef.getFlags().isSet(peerAccCtx.get(), flags);
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "access snapshot " + getSnapshotDfnDescriptionInline(snapDfnRef),
                ApiConsts.FAIL_ACC_DENIED_SNAP_DFN
            );
        }
        return ret;
    }

    private void markSnapshotDfnDeleted(SnapshotDefinition snapshotDfn)
    {
        try
        {
            // first remove snapDfn from backupQueueItems where it is a prevSnap
            backupInfoMgr.deletePrevSnapFromQueueItems(snapshotDfn);
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

    private void deleteSnapshotDfnPrivileged(SnapshotDefinition snapshotDfn)
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
