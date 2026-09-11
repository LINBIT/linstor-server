package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.backupshipping.BackupShippingUtils;
import com.linbit.linstor.core.BackupInfoManager;
import com.linbit.linstor.core.SharedResourceManager;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSuccessUtils;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;
import com.linbit.utils.StringUtils;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotApiCallHandler.getSnapshotDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotApiCallHandler.getSnapshotDfnDescription;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotApiCallHandler.getSnapshotDfnDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotApiCallHandler.makeSnapshotContext;

import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.regex.Pattern;

import org.slf4j.MDC;
import reactor.core.publisher.Flux;

@Singleton
public class CtrlSnapshotDeleteApiCallHandler implements CtrlSatelliteConnectionListener
{
    private final ScopeRunner scopeRunner;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final ResponseConverter responseConverter;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlPropsHelper propsHelper;
    private final ErrorReporter errorReporter;
    private final BackupInfoManager backupInfoMgr;
    private final SharedResourceManager sharedRscMgr;
    // Provider breaks a Guice construction cycle:
    // CtrlRscDfnDeleteApiCallHandler -> CtrlRscDfnTruncateApiCallHandler -> AutoSnapshotTask ->
    // CtrlSnapshotCrtApiCallHandler -> CtrlSnapshotDeleteApiCallHandler -> (here)
    private final Provider<CtrlRscDfnDeleteApiCallHandler> ctrlRscDfnDeleteApiCallHandlerProvider;

    @Inject
    public CtrlSnapshotDeleteApiCallHandler(
        ScopeRunner scopeRunnerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        ResponseConverter responseConverterRef,
        LockGuardFactory lockguardFactoryRef,
        CtrlPropsHelper propsHelperRef,
        ErrorReporter errorReporterRef,
        BackupInfoManager backupInfoMgrRef,
        SharedResourceManager sharedRscMgrRef,
        Provider<CtrlRscDfnDeleteApiCallHandler> ctrlRscDfnDeleteApiCallHandlerProviderRef
    )
    {
        scopeRunner = scopeRunnerRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        responseConverter = responseConverterRef;
        lockGuardFactory = lockguardFactoryRef;
        propsHelper = propsHelperRef;
        errorReporter = errorReporterRef;
        backupInfoMgr = backupInfoMgrRef;
        sharedRscMgr = sharedRscMgrRef;
        ctrlRscDfnDeleteApiCallHandlerProvider = ctrlRscDfnDeleteApiCallHandlerProviderRef;
    }

    @Override
    public Collection<Flux<ApiCallRc>> resourceDefinitionConnected(ResourceDefinition rscDfn, ResponseContext context)
    {
        List<Flux<ApiCallRc>> fluxes = new ArrayList<>();

        for (SnapshotDefinition snapshotDfn : rscDfn.getSnapshotDfns())
        {
            if (snapshotDfn.getFlags().isSet(SnapshotDefinition.Flags.DELETE))
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
     * @param deleteEmptyRscDfn if true, the resource definition is deleted as well when it has
     * neither resources nor snapshots left after this snapshot has been deleted. The check and the
     * resource-definition deletion happen atomically, so a concurrently created resource or snapshot
     * prevents the deletion.
     *
     * @return deletion-flux
     */
    public Flux<ApiCallRc> deleteSnapshot(
        String rscNameStr,
        String snapshotNameStr,
        @Nullable List<String> nodeNamesStrListRef,
        boolean deleteEmptyRscDfn
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
            ResourceName rscName = LinstorParsingUtils.asRscName(rscNameStr);
            ret = deleteSnapshot(
                rscName,
                LinstorParsingUtils.asSnapshotName(snapshotNameStr),
                nodeNamesStrListRef
            )
                .transform(responses -> responseConverter.reportingExceptions(context, responses));
            if (deleteEmptyRscDfn)
            {
                ret = ret.concatWith(
                    ctrlRscDfnDeleteApiCallHandlerProvider.get().deleteResourceDefinitionIfEmpty(rscName)
                );
            }
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
     *
     * @return deletion-flux or error-flux in case of exception
     */
    public Flux<ApiCallRc> deleteSnapshot(
        ResourceName rscName,
        SnapshotName snapshotName,
        @Nullable Collection<String> nodeNamesStrListRef
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
        @Nullable Collection<String> nodeNamesStrListRef
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        @Nullable SnapshotDefinition snapshotDfn = ctrlApiDataLoader.loadSnapshotDfnOrNull(
            rscNameRef,
            snapshotNameRef
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

        Flux<ApiCallRc> phase2Flux = Flux.empty();
        if (nodeNamesStrListRef == null || nodeNamesStrListRef.isEmpty())
        {
            /*
             * Snapshots in shared storage pools exist once on the shared data but are registered on
             * every node holding a copy of the resource. Only the node with the active copy may remove
             * the backing snapshot (an inactive node's lvremove would interfere with the active peer's
             * kernel mappings, e.g. thick LVM activates origin and snapshots together), so the deletion
             * is staggered: the "executor" snapshots are deleted first, the remaining per-node objects
             * afterwards - their satellites then find the backing snapshot already gone.
             */
            List<Snapshot> deferredSnaps = new ArrayList<>();
            List<Snapshot> executorSnaps = new ArrayList<>();
            for (Snapshot snapshot : getAllSnapshots(snapshotDfn))
            {
                @Nullable Resource rsc = snapshotDfn.getResourceDefinition()
                    .getResource(snapshot.getNodeName());
                boolean deferred;
                if (rsc != null)
                {
                    deferred = sharedRscMgr.isInactiveShared(rsc);
                }
                else
                {
                    // no resource on the snapshot's node: defer if the snapshot lives on a shared
                    // pool and another node holds the active copy (the executor)
                    deferred = sharedRscMgr.isBackedBySharedStorPool(snapshot);
                }
                if (deferred)
                {
                    deferredSnaps.add(snapshot);
                }
                else
                {
                    executorSnaps.add(snapshot);
                }
            }

            /*
             * Shared storage pools without LINSTOR locking rely on an external lock manager
             * (e.g. lvmlockd), which only serializes the individual storage commands, not the
             * satellite's exists-check + remove sequence: two nodes deleting the same backing
             * snapshot concurrently would race, unlike on LINSTOR-locked pools, where the
             * controller serializes the satellites via the shared storage pool locks. Ensure
             * every externally locked shared space has an executor - without an active copy any
             * node may remove the backing snapshot - so only a single node ever touches it.
             */
            Set<SharedStorPoolName> executorSharedSpaces = new TreeSet<>();
            for (Snapshot snapshot : executorSnaps)
            {
                executorSharedSpaces.addAll(sharedRscMgr.getSharedSpNames(snapshot));
            }
            Iterator<Snapshot> deferredSnapsIt = deferredSnaps.iterator();
            while (deferredSnapsIt.hasNext())
            {
                Snapshot snapshot = deferredSnapsIt.next();
                Set<SharedStorPoolName> uncoveredSpaces = sharedRscMgr.getExternallyLockedSpNames(snapshot);
                uncoveredSpaces.removeAll(executorSharedSpaces);
                if (!uncoveredSpaces.isEmpty())
                {
                    executorSharedSpaces.addAll(sharedRscMgr.getSharedSpNames(snapshot));
                    executorSnaps.add(snapshot);
                    deferredSnapsIt.remove();
                }
            }

            if (executorSnaps.isEmpty() || deferredSnaps.isEmpty())
            {
                // no active copy at all (nothing holds kernel mappings, any node may delete) or
                // nothing to defer: delete everything at once
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
                for (Snapshot snapshot : executorSnaps)
                {
                    responses.addEntry(
                        "Marked snapshot for deletion " + getSnapshotDescriptionInline(snapshot),
                        ApiConsts.DELETED
                    );
                    markSnapshotDeleted(snapshot);
                }
                phase2Flux = deleteDeferredSharedSnapshots(rscName, snapshotName);
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
            .concatWith(deleteSnapshotsOnNodes(rscName, snapshotName))
            .concatWith(phase2Flux);
    }

    /**
     * Second phase of a staggered shared-SP snapshot deletion: once the executor node removed the
     * backing snapshot (and its per-node objects are gone), the remaining per-node snapshot objects
     * are deleted as well - their satellites find the backing snapshot already removed.
     */
    private Flux<ApiCallRc> deleteDeferredSharedSnapshots(ResourceName rscName, SnapshotName snapshotName)
    {
        return scopeRunner.fluxInTransactionalScope(
            "Delete deferred shared snapshots",
            lockGuardFactory.buildDeferred(LockType.WRITE, LockObj.RSC_DFN_MAP),
            () -> deleteDeferredSharedSnapshotsInTransaction(rscName, snapshotName),
            MDC.getCopyOfContextMap()
        );
    }

    private Flux<ApiCallRc> deleteDeferredSharedSnapshotsInTransaction(
        ResourceName rscName,
        SnapshotName snapshotName
    )
    {
        Flux<ApiCallRc> flux = Flux.empty();
        @Nullable SnapshotDefinition snapshotDfn = ctrlApiDataLoader.loadSnapshotDfnOrNull(rscName, snapshotName);
        if (snapshotDfn != null)
        {
            boolean executorPending = false;
            for (Snapshot snapshot : getAllSnapshots(snapshotDfn))
            {
                if (isFlagSet(snapshot, Snapshot.Flags.DELETE))
                {
                    executorPending = true;
                    break;
                }
            }
            if (executorPending)
            {
                // the executor could not confirm the deletion of the backing snapshot (e.g. satellite
                // offline or failed): keep the remaining per-node objects, a retry of the deletion
                // cleans them up once the backing snapshot is gone
                flux = Flux.just(
                    ApiCallRcImpl.singleApiCallRc(
                        ApiConsts.WARN_NOT_CONNECTED,
                        "Deletion of the backing snapshot was not confirmed yet; the snapshot " +
                            "objects of the remaining nodes are kept until the deletion is retried"
                    )
                );
            }
            else
            {
                ApiCallRcImpl responses = new ApiCallRcImpl();
                markSnapshotDfnDeleted(snapshotDfn);
                for (Snapshot snapshot : getAllSnapshots(snapshotDfn))
                {
                    responses.addEntry(
                        "Marked snapshot for deletion " + getSnapshotDescriptionInline(snapshot),
                        ApiConsts.DELETED
                    );
                    markSnapshotDeleted(snapshot);
                }
                ctrlTransactionHelper.commit();

                flux = Flux.<ApiCallRc>just(responses)
                    .concatWith(deleteSnapshotsOnNodes(rscName, snapshotName));
            }
        }
        return flux;
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
        return BackupShippingUtils.isAnyShippingInProgress(snapshotDfnRef);
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
        @Nullable SnapshotDefinition snapshotDfn = ctrlApiDataLoader.loadSnapshotDfnOrNull(rscName, snapshotName);

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
        @Nullable SnapshotDefinition snapshotDfn = ctrlApiDataLoader.loadSnapshotDfnOrNull(rscName, snapshotName);

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
                for (SnapshotDefinition snapDfn : rscDfnRef.getSnapshotDfns())
                {
                    if (
                        snapDfn.getFlags().isSet(snapDfnFilterFlag) &&
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
                null,
                "Invalid value for property " + rscDfnPropNameSpc + "/" + rscDfnPropKeepKey
            );
        }
        ctrlTransactionHelper.commit();

        return flux;
    }

    private boolean isFlagSet(Snapshot snapshotRef, Snapshot.Flags... flags)
    {
        boolean ret;
        ret = snapshotRef.getFlags().isSet(flags);
        return ret;
    }

    private boolean isFlagSet(SnapshotDefinition snapDfnRef, SnapshotDefinition.Flags... flags)
    {
        boolean ret;
        ret = snapDfnRef.getFlags().isSet(flags);
        return ret;
    }

    private void markSnapshotDfnDeleted(SnapshotDefinition snapshotDfn)
    {
        try
        {
            // first remove snapDfn from backupQueueItems where it is a prevSnap
            backupInfoMgr.deletePrevSnapFromQueueItems(snapshotDfn);
            snapshotDfn.markDeleted();
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
            snapshot.markDeleted();
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
    }

    private Collection<Snapshot> getAllSnapshots(SnapshotDefinition snapshotDfn)
    {
        Collection<Snapshot> allSnapshots;
        allSnapshots = snapshotDfn.getAllSnapshots();
        return allSnapshots;
    }

    private Collection<Snapshot> getAllSnapshotsPrivileged(SnapshotDefinition snapshotDfn)
    {
        Collection<Snapshot> allSnapshots;
        allSnapshots = snapshotDfn.getAllSnapshots();
        return allSnapshots;
    }

    private void deleteSnapshotDfnPrivileged(SnapshotDefinition snapshotDfn)
    {
        try
        {
            snapshotDfn.delete();
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
            snapshot.delete();
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
    }
}
