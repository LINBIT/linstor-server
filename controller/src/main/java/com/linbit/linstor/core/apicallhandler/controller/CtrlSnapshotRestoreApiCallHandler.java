package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.backupshipping.BackupShippingUtils;
import com.linbit.linstor.core.BackupInfoManager;
import com.linbit.linstor.core.SharedResourceManager;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.autohelper.AutoHelperContext;
import com.linbit.linstor.core.apicallhandler.controller.autohelper.CtrlRscAutoHelper;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.apicallhandler.response.OperationDescription;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.event.EventStreamClosedException;
import com.linbit.linstor.event.EventStreamTimeoutException;
import com.linbit.linstor.layer.LayerPayload;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscObject.DrbdRscFlags;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.utils.layer.DrbdLayerUtils;
import com.linbit.linstor.utils.layer.LayerRscUtils;
import com.linbit.linstor.utils.layer.LayerVlmUtils;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;

import static com.linbit.utils.StringUtils.firstLetterCaps;

import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;

import reactor.core.publisher.Flux;

@Singleton
public class CtrlSnapshotRestoreApiCallHandler
{
    private final ErrorReporter errorReporter;
    private final ScopeRunner scopeRunner;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlSnapshotHelper ctrlSnapshotHelper;
    private final CtrlRscCrtApiHelper ctrlRscCrtApiHelper;
    private final CtrlVlmCrtApiHelper ctrlVlmCrtApiHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final ResponseConverter responseConverter;
    private final Provider<Peer> peer;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlRscAutoHelper autoHelper;
    private final CtrlPropsHelper ctrlPropsHelper;
    private final BackupInfoManager backupInfoMgr;
    private final SharedResourceManager sharedRscMgr;
    private final CtrlSatelliteUpdateCaller ctrlStltUpdateCaller;
    private final CtrlRscAutoBalanceHelper ctrlRscAutoBalanceHelper;

    @Inject
    public CtrlSnapshotRestoreApiCallHandler(
        ErrorReporter errorReporterRef,
        ScopeRunner scopeRunnerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlSnapshotHelper ctrlSnapshotHelperRef,
        CtrlRscCrtApiHelper ctrlRscCrtApiHelperRef,
        CtrlVlmCrtApiHelper ctrlVlmCrtApiHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        ResponseConverter responseConverterRef,
        Provider<Peer> peerRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlRscAutoHelper ctrlRscAutoHelperRef,
        CtrlPropsHelper ctrlPropsHelperRef,
        BackupInfoManager backupInfoMgrRef,
        SharedResourceManager sharedRscMgrRef,
        CtrlSatelliteUpdateCaller ctrlStltUpdateCallerRef,
        CtrlRscAutoBalanceHelper ctrlRscAutoBalanceHelperRef
    )
    {
        errorReporter = errorReporterRef;
        scopeRunner = scopeRunnerRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlSnapshotHelper = ctrlSnapshotHelperRef;
        ctrlRscCrtApiHelper = ctrlRscCrtApiHelperRef;
        ctrlVlmCrtApiHelper = ctrlVlmCrtApiHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        responseConverter = responseConverterRef;
        peer = peerRef;
        lockGuardFactory = lockGuardFactoryRef;
        autoHelper = ctrlRscAutoHelperRef;
        ctrlPropsHelper = ctrlPropsHelperRef;
        backupInfoMgr = backupInfoMgrRef;
        sharedRscMgr = sharedRscMgrRef;
        ctrlStltUpdateCaller = ctrlStltUpdateCallerRef;
        ctrlRscAutoBalanceHelper = ctrlRscAutoBalanceHelperRef;
    }

    private ResponseContext makeSnapshotRestoreContext(String rscNameStr)
    {
        Map<String, String> objRefs = new TreeMap<>();
        objRefs.put(ApiConsts.KEY_RSC_DFN, rscNameStr);

        return new ResponseContext(
            ApiOperation.makeRegisterOperation(),
            "Resource: '" + rscNameStr + "'",
            "resource '" + rscNameStr,
            ApiConsts.MASK_SNAPSHOT,
            objRefs
        );
    }

    public Flux<ApiCallRc> restoreSnapshot(
        List<String> nodeNameStrs,
        String fromRscNameStr,
        String fromSnapshotNameStr,
        String toRscNameStr,
        Map<String, String> renameStorPoolMap
    )
    {
        ResponseContext context = makeSnapshotRestoreContext(toRscNameStr);
        Flux<ApiCallRc> ret;
        try
        {
            ret = restoreSnapshot(
                nodeNameStrs,
                LinstorParsingUtils.asRscName(fromRscNameStr),
                LinstorParsingUtils.asSnapshotName(fromSnapshotNameStr),
                LinstorParsingUtils.asRscName(toRscNameStr),
                renameStorPoolMap
            ).transform(responses -> responseConverter.reportingExceptions(context, responses));
        }
        catch (ApiRcException exc)
        {
            ret = Flux.error(exc);
        }
        return ret;
    }

    public Flux<ApiCallRc> restoreSnapshot(
        List<String> nodeNameStrs,
        ResourceName fromRscName,
        SnapshotName fromSnapshotName,
        ResourceName toRscName,
        Map<String, String> renameStorPoolMap
    )
    {
        return scopeRunner.fluxInTransactionalScope(
            "Restore Snapshot Resource",
            lockGuardFactory.createDeferred()
                .read(LockObj.NODES_MAP)
                .write(LockObj.RSC_DFN_MAP)
                .build(),
            () -> restoreResourceInTransaction(
                nodeNameStrs,
                fromRscName,
                fromSnapshotName,
                toRscName,
                false,
                true,
                renameStorPoolMap
            )
        );
    }

    public Flux<ApiCallRc> restoreSnapshotForRollback(
        List<String> nodeNameStrs,
        ResourceName fromRscName,
        SnapshotName fromSnapshotName,
        ResourceName toRscName,
        Map<String, String> renameStorPoolMap
    )
    {
        return scopeRunner.fluxInTransactionalScope(
            "Restore Snapshot Resource for Rollback",
            lockGuardFactory.createDeferred()
                .read(LockObj.NODES_MAP)
                .write(LockObj.RSC_DFN_MAP)
                .build(),
            () -> restoreResourceInTransaction(
                nodeNameStrs,
                fromRscName,
                fromSnapshotName,
                toRscName,
                false,
                false,
                renameStorPoolMap
            )
        );
    }

    public Flux<ApiCallRc> restoreSnapshotFromBackup(
        List<String> nodeNameStrs,
        SnapshotName fromSnapshotName,
        ResourceName toRscName
    )
    {
        ResponseContext context = makeSnapshotRestoreContext(toRscName.displayValue);
        return scopeRunner.fluxInTransactionalScope(
            "Restore Snapshot Resource from backup",
            lockGuardFactory.createDeferred()
                .read(LockObj.NODES_MAP)
                .write(LockObj.RSC_DFN_MAP)
                .build(),
            () -> restoreResourceInTransaction(
                nodeNameStrs,
                toRscName,
                fromSnapshotName,
                toRscName,
                true,
                false,
                Collections.emptyMap() // rename-storpool already happened during download
            )
        ).transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    /**
     * Returns the snapshots to restore from when no nodes were given. Snapshots without shared
     * storage pools restore on every node holding the snapshot (each node has its own snapshot
     * data). Per-node snapshots of the same shared space all refer to the same data, so exactly one
     * of them is chosen per shared space - preferably on the node with the active resource copy,
     * since reading a thick snapshot implicitly activates its origin.
     */
    private Collection<Snapshot> selectRestoreSnapshots(SnapshotDefinition fromSnapshotDfn)
    {
        List<Snapshot> ret = new ArrayList<>();
        Map<Set<SharedStorPoolName>, Snapshot> sharedExecutors = new HashMap<>();
        Map<Set<SharedStorPoolName>, Boolean> sharedExecutorsActive = new HashMap<>();
        for (Snapshot snapshot : new TreeSet<>(fromSnapshotDfn.getAllSnapshots()))
        {
            Set<SharedStorPoolName> sharedSpNames = sharedRscMgr.getSharedSpNames(snapshot);
            if (!sharedSpNames.isEmpty())
            {
                @Nullable Resource rsc = fromSnapshotDfn.getResourceDefinition()
                    .getResource(snapshot.getNodeName());
                boolean active = rsc != null &&
                    !rsc.getStateFlags().isSomeSet(
                        Resource.Flags.INACTIVE,
                        Resource.Flags.INACTIVE_PERMANENTLY
                    );
                @Nullable Snapshot executor = sharedExecutors.get(sharedSpNames);
                if (executor == null || (active && !sharedExecutorsActive.get(sharedSpNames)))
                {
                    sharedExecutors.put(sharedSpNames, snapshot);
                    sharedExecutorsActive.put(sharedSpNames, active);
                }
            }
            else
            {
                ret.add(snapshot);
            }
        }
        ret.addAll(sharedExecutors.values());
        return ret;
    }

    private Flux<ApiCallRc> restoreResourceInTransaction(
        List<String> nodeNameStrs,
        ResourceName fromRscName,
        SnapshotName fromSnapshotName,
        ResourceName toRscName,
        boolean fromBackup,
        boolean fromApi,
        Map<String, String> renameStorPoolMap
    )
    {
        Flux<ApiCallRc> deploymentResponses = Flux.just();
        Flux<ApiCallRc> cleanupPropertiesFlux = Flux.empty();
        Flux<ApiCallRc> autoFlux;
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = new ResponseContext(
            new ApiOperation(ApiConsts.MASK_CRT, new OperationDescription("restore", "restoring")),
            getSnapshotRestoreDescription(nodeNameStrs, toRscName.displayValue),
            getSnapshotRestoreDescriptionInline(nodeNameStrs, toRscName.displayValue),
            ApiConsts.MASK_RSC,
            Collections.emptyMap()
        );

        try
        {
            ResourceDefinition fromRscDfn = ctrlApiDataLoader.loadRscDfn(fromRscName);

            SnapshotDefinition fromSnapshotDfn = ctrlApiDataLoader.loadSnapshotDfn(fromRscDfn, fromSnapshotName);

            ResourceDefinition toRscDfn = ctrlApiDataLoader.loadRscDfn(toRscName);

            if (toRscDfn.getResourceCount() != 0)
            {
                throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_EXISTS_RSC,
                    "Cannot restore to resource definition which already has resources"
                ));
            }

            if (BackupShippingUtils.isAnyShippingInProgress(fromSnapshotDfn))
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_EXISTS_SNAPSHOT_SHIPPING,
                        "Snapshot is being shipped. Please wait until shipping is finished"
                    )
                );
            }

            if (!fromBackup && backupInfoMgr.restoreContainsRscDfn(toRscDfn))
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_IN_USE,
                        fromSnapshotName + " is currently being restored from a backup. " +
                            "Please wait until the restore is finished"
                    )
                );
            }

            ctrlSnapshotHelper.ensureSnapshotSuccessful(fromSnapshotDfn);

            ctrlPropsHelper.copy(
                ctrlPropsHelper.getProps(fromSnapshotDfn, true),
                ctrlPropsHelper.getProps(toRscDfn)
            );

            Set<Resource> restoredResources = new TreeSet<>();

            if (nodeNameStrs.isEmpty())
            {
                for (Snapshot snapshot : selectRestoreSnapshots(fromSnapshotDfn))
                {
                    restoredResources.add(
                        restoreOnNode(
                            fromSnapshotDfn,
                            toRscDfn,
                            snapshot.getNode(),
                            fromBackup,
                            renameStorPoolMap,
                            responses
                        )
                    );
                }
            }
            else
            {
                for (String nodeNameStr : nodeNameStrs)
                {
                    Node node = ctrlApiDataLoader.loadNode(nodeNameStr);
                    restoredResources.add(
                        restoreOnNode(fromSnapshotDfn, toRscDfn, node, fromBackup, renameStorPoolMap, responses)
                    );
                }
            }

            // clear possible EBS properties
            for (Resource rsc : restoredResources)
            {
                Iterator<Volume> vlmIt = rsc.iterateVolumes();
                while (vlmIt.hasNext())
                {
                    Volume vlm = vlmIt.next();
                    vlm.getProps().removeNamespace(
                        ApiConsts.NAMESPC_STLT + "/" + ApiConsts.NAMESPC_EBS
                    );
                }
            }

            autoFlux = autoHelper.manage(new AutoHelperContext(responses, context, toRscDfn))
                .flux();

            checkDrbdInitializedState(toRscDfn);

            Flux<ApiCallRc> balanceFlux = ctrlRscAutoBalanceHelper.balanceAfterOperation(
                toRscDfn, ApiConsts.KEY_BALANCE_AFTER_RESTORE, ApiConsts.NAMESPC_SNAPSHOT);

            ctrlTransactionHelper.commit();

            if (toRscDfn.getVolumeDfnCount() == 0)
            {
                responseConverter.addWithDetail(responses, context, ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.WARN_NOT_FOUND,
                        "No volumes to restore."
                    )
                    .setDetails("The target resource definition has no volume definitions. " +
                        "The restored resources will be empty.")
                    .setCorrection("Restore the volume definitions to the target resource definition.")
                    .build()
                );
            }

            deploymentResponses = ctrlRscCrtApiHelper.deployResources(context, restoredResources);
            cleanupPropertiesFlux = cleanupProperties(restoredResources);

            responseConverter.addWithOp(responses, context, ApiCallRcImpl
                .entryBuilder(
                    ApiConsts.CREATED,
                    firstLetterCaps(getSnapshotRestoreDescriptionInline(nodeNameStrs, toRscName.displayValue)) +
                        " restored " +
                        "from resource '" + fromRscName + "', snapshot '" + fromSnapshotName + "'."
                )
                .setDetails("Resource UUIDs: " +
                    toRscDfn.streamResource()
                        .map(Resource::getUuid)
                        .map(UUID::toString)
                        .collect(Collectors.joining(", ")))
                .build()
            );

            final Flux<ApiCallRc> cleanupFlux = cleanupPropertiesFlux;

            return Flux.<ApiCallRc>just(responses)
                .concatWith(deploymentResponses)
                .concatWith(autoFlux)
                .concatWith(balanceFlux)
                .concatWith(cleanupFlux)
                .onErrorResume(CtrlResponseUtils.DelayedApiRcException.class, ignored -> cleanupFlux)
                .onErrorResume(
                    EventStreamTimeoutException.class,
                    ignored -> Flux.just(ctrlRscCrtApiHelper.makeResourceDidNotAppearMessage(context))
                        .concatWith(cleanupFlux)
                )
                .onErrorResume(
                    EventStreamClosedException.class,
                    ignored -> Flux.just(ctrlRscCrtApiHelper.makeEventStreamDisappearedUnexpectedlyMessage(context))
                        .concatWith(cleanupFlux)
                );
        }
        catch (Exception | ImplementationError exc)
        {
            if (fromApi)
            {
                return Flux.just(responseConverter.reportException(peer.get(), context, exc));
            }
            else
            {
                return Flux.error(exc);
            }
        }
    }

    /**
     * <p>Sets the {@value InternalApiConsts#KEY_LINSTOR_DRBD_INITIAL_UPTODATE_ON} property to the nodeName
     * of the first Snapshot (which has to have a disk) of the given SnapshotDefinition.</p>
     *
     * <p>If this SnapshotDefinition is a backup, we also unset the {@link VolumeDefinition.Flags#DRBD_INITIALIZED}
     * flag. Otherwise we ensure the same flag is set.</p>
     *
     * <p>Backup path: When a snapshot was received as a backup, it is marked to re-create its local metadata.
     * Before the snapshot was sent the new DRBD_INITIALIZED flag and the VlmDfn property InitialUpToDateOn were
     * cleared. If DRBD_INITIALZED would be set, no peer would even read the property (on purpose).
     * Without the property, no peer would think of itself as a winner, thus the DRBD resource would never
     * become UpToDate.
     * To avoid this, this method sets the InitialUpToDateOn property to let the first resource win the
     * race, so it can become UpToDate and therefore also pull the other peers eventually into UpToDate state. </p>
     *
     * <p>If the given SnapDfn had also non-backup snapshots, those should be able to keep their metadata.
     * In that case we do not have to do anything. If however all snapshots are descendants from backups
     * we must pick one and decide the UpToDate race upfront so that the winner can set its local DRBD
     * as UpToDate, which also triggers the controller to set the DRBD_INITIALZED flag eventually.</p>
     *
     * <p>Non-Backup path: When a regular snapshot (i.e. not from a backup) is restored, we can or even should
     * assume that the backing snapshot LVs have correct metadata which do not need be be initialized. When
     * a SnapshotVolumeDefinition is created it unfortunately does not properly store the VolumeDefinition's flags,
     * therefore the restored VolumeDefinition will also not have the DRBD_INITIALIZED flag set, although it
     * refers to a device with healthy and UpToDate metadata. To fix this issue we simply set the flag in this case.</p>
     */
    private void checkDrbdInitializedState(ResourceDefinition rscDfnRef)
    {
        if (rscDfnRef.getDiskfulCount() > 0)
        {
            String winnerNodeName = rscDfnRef.getDiskfulResources().get(0).getNode().getName().value;
            boolean uninitializeDrbd = DrbdLayerUtils.isForceInitialSyncSet(rscDfnRef);
            Iterator<VolumeDefinition> vlmDfnIt = rscDfnRef.iterateVolumeDfn();
            while (vlmDfnIt.hasNext())
            {
                VolumeDefinition vlmDfn = vlmDfnIt.next();
                try
                {
                    vlmDfn.getProps()
                        .setProp(InternalApiConsts.KEY_LINSTOR_DRBD_INITIAL_UPTODATE_ON, winnerNodeName);
                    if (uninitializeDrbd)
                    {
                        vlmDfn.getFlags().disableFlags(VolumeDefinition.Flags.DRBD_INITIALIZED);
                    }
                    else
                    {
                        vlmDfn.getFlags().enableFlags(VolumeDefinition.Flags.DRBD_INITIALIZED);
                    }
                }
                catch (InvalidKeyException | InvalidValueException exc)
                {
                    throw new ImplementationError(exc);
                }
                catch (DatabaseException dbExc)
                {
                    throw new ApiDatabaseException(dbExc);
                }
            }
        }
    }

    public Flux<ApiCallRc> unsetRestoreTarget(ResourceName rscNameRef)
    {
        ResponseContext context = makeSnapshotRestoreContext(rscNameRef.displayValue);
        return scopeRunner.fluxInTransactionalScope(
            "Remove RESTORE_TARGET",
            lockGuardFactory.createDeferred()
                .write(LockObj.RSC_DFN_MAP)
                .build(),
            () -> unsetRestoreTargetInTransaction(rscNameRef)
        ).transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> unsetRestoreTargetInTransaction(ResourceName rscNameRef)
    {
        @Nullable ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfnOrNull(rscNameRef);
        Flux<ApiCallRc> ret;
        if (rscDfn != null)
        {
            unsetFlags(rscDfn, ResourceDefinition.Flags.RESTORE_TARGET);
            ctrlTransactionHelper.commit();
            ret = ctrlStltUpdateCaller.updateSatellites(rscDfn, Flux.empty())
                .transform(
                    responses -> CtrlResponseUtils.combineResponses(
                        errorReporter,
                        responses,
                        rscDfn.getName(),
                        "Removed RESTORE_TARGET flag of {1} on {0}"
                    )
                );
        }
        else
        {
            ret = Flux.empty();
        }
        return ret;
    }

    private void unsetFlags(ResourceDefinition rscDfn, ResourceDefinition.Flags... flags)
    {
        try
        {
            rscDfn.getFlags().disableFlags(flags);
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
    }

    private Flux<ApiCallRc> cleanupProperties(Set<Resource> restoredResourcesRef)
    {
        return scopeRunner.fluxInTransactionalScope(
            "Cleanup restore-properties",
            lockGuardFactory.createDeferred()
                .read(LockObj.NODES_MAP)
                .write(LockObj.RSC_DFN_MAP)
                .build(),
            () -> cleanupPropertiesInTransaction(restoredResourcesRef)
        );
    }

    private Flux<ApiCallRc> cleanupPropertiesInTransaction(Set<Resource> restoredResourcesRef)
    {
        try
        {
            for (Resource rsc : restoredResourcesRef)
            {
                Iterator<Volume> iterateVolumes = rsc.iterateVolumes();
                while (iterateVolumes.hasNext())
                {
                    Volume vlm = iterateVolumes.next();
                    Props props = vlm.getProps();
                    props.removeProp(ApiConsts.KEY_VLM_RESTORE_FROM_RESOURCE);
                    props.removeProp(ApiConsts.KEY_VLM_RESTORE_FROM_SNAPSHOT);
                }
            }
            ctrlTransactionHelper.commit();
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
        return Flux.empty();
    }

    private Resource restoreOnNode(
        SnapshotDefinition fromSnapshotDfn,
        ResourceDefinition toRscDfn,
        Node node,
        boolean fromBackup,
        Map<String, String> renameStorPoolMap,
        @Nullable ApiCallRc apiCallRc
    )
        throws InvalidKeyException, InvalidValueException, DatabaseException
    {
        Snapshot snapshot = ctrlApiDataLoader.loadSnapshot(node, fromSnapshotDfn);

        boolean copyIntoVlmDfn = toRscDfn.getResourceCount() == 0;

        Resource rsc = ctrlRscCrtApiHelper.createResourceFromSnapshot(
            toRscDfn,
            node,
            snapshot,
            fromBackup,
            renameStorPoolMap,
            apiCallRc
        );

        ctrlPropsHelper.copy(
            ctrlPropsHelper.getProps(snapshot, true),
            ctrlPropsHelper.getProps(rsc)
        );
        StateFlags<Flags> rscFlags = rsc.getStateFlags();
        rscFlags.enableFlags(Resource.Flags.RESTORE_FROM_SNAPSHOT);

        Iterator<VolumeDefinition> toVlmDfnIter = ctrlRscCrtApiHelper.getVlmDfnIterator(toRscDfn);
        while (toVlmDfnIter.hasNext())
        {
            VolumeDefinition toVlmDfn = toVlmDfnIter.next();
            VolumeNumber volumeNumber = toVlmDfn.getVolumeNumber();

            SnapshotVolumeDefinition fromSnapshotVlmDfn =
                fromSnapshotDfn.getSnapshotVolumeDefinition(volumeNumber);

            if (fromSnapshotVlmDfn == null)
            {
                throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_NOT_FOUND_SNAPSHOT_VLM_DFN,
                    "Snapshot does not contain required volume number " + volumeNumber
                ));
            }

            if (copyIntoVlmDfn)
            {
                ctrlPropsHelper.copy(
                    ctrlPropsHelper.getProps(fromSnapshotVlmDfn, true),
                    ctrlPropsHelper.getProps(toVlmDfn)
                );
            }

            long snapshotVolumeSize = fromSnapshotVlmDfn.getVolumeSize();
            long requiredVolumeSize = toVlmDfn.getVolumeSize();
            if (snapshotVolumeSize != requiredVolumeSize)
            {
                throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_VLM_SIZE,
                    "Snapshot size does not match for volume number " + volumeNumber.value + "; " +
                        "snapshot size: " + snapshotVolumeSize + "KiB, " +
                        "required size: " + requiredVolumeSize + "KiB"
                ));
            }

            SnapshotVolume fromSnapshotVolume = snapshot.getVolume(volumeNumber);

            if (fromSnapshotVolume == null)
            {
                throw new ImplementationError("Expected snapshot volume missing");
            }

            Map<String, StorPool> storPool = LayerVlmUtils.getStorPoolMap(snapshot, volumeNumber);
            LayerPayload payload = new LayerPayload();
            for (Entry<String, StorPool> storPoolEntry : storPool.entrySet())
            {
                if (storPoolEntry.getKey().equals(RscLayerSuffixes.SUFFIX_DATA))
                {
                    payload.putStorageVlmPayload(storPoolEntry.getKey(), volumeNumber.value, storPoolEntry.getValue());
                }
            }
            Volume toVlm = ctrlVlmCrtApiHelper
                .createVolumeFromAbsVolume(
                    rsc,
                    toVlmDfn,
                    payload,
                    null,
                    fromSnapshotVolume,
                    renameStorPoolMap,
                    apiCallRc
                );

            Props vlmProps = ctrlPropsHelper.getProps(toVlm);
            ctrlPropsHelper.copy(
                ctrlPropsHelper.getProps(fromSnapshotVolume, true),
                vlmProps
            );
            vlmProps.setProp(
                ApiConsts.KEY_VLM_RESTORE_FROM_RESOURCE, fromSnapshotVlmDfn.getResourceName().displayValue
            );
            vlmProps.setProp(
                ApiConsts.KEY_VLM_RESTORE_FROM_SNAPSHOT, fromSnapshotVlmDfn.getSnapshotName().displayValue
            );
        }
        Set<AbsRscLayerObject<Resource>> drbdLayers = LayerRscUtils.getRscDataByLayer(
            rsc.getLayerData(),
            DeviceLayerKind.DRBD
        );
        boolean forceInitSync = false;
        for (AbsRscLayerObject<Resource> layer : drbdLayers)
        {
            DrbdRscData<Resource> drbdRscData = (DrbdRscData<Resource>) layer;
            if (DrbdLayerUtils.isForceInitialSyncSet(drbdRscData))
            {
                forceInitSync = true;
                drbdRscData.getFlags().disableFlags(DrbdRscFlags.INITIALIZED);
                drbdRscData.getFlags().enableFlags(
                    DrbdRscFlags.FROM_BACKUP, DrbdRscFlags.FORCE_NEW_METADATA
                );
            }
        }
        if (forceInitSync)
        {
            rsc.getResourceDefinition().getProps().removeProp(InternalApiConsts.DEPRECATED_PROP_PRIMARY_SET);
        }
        unsetFlags(toRscDfn, ResourceDefinition.Flags.RESTORE_TARGET);

        return rsc;
    }

    private static String getSnapshotRestoreDescription(List<String> nodeNameStrs, String toRscNameStr)
    {
        return nodeNameStrs.isEmpty() ?
            "Resource: " + toRscNameStr :
            "Nodes: " + String.join(", ", nodeNameStrs) + "; Resource: " + toRscNameStr;
    }

    private static String getSnapshotRestoreDescriptionInline(List<String> nodeNameStrs, String toRscNameStr)
    {
        return nodeNameStrs.isEmpty() ?
            "resource '" + toRscNameStr + "'" :
            "resource '" + toRscNameStr + "' on nodes '" + String.join(", ", nodeNameStrs) + "'";
    }
}
