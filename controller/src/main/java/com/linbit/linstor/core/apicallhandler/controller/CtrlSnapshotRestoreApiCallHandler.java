package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.BackupInfoManager;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscAutoHelper.AutoHelperContext;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.apicallhandler.response.OperationDescription;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.identifier.ResourceName;
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
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
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

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Collections;
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
    private final Provider<AccessContext> peerAccCtx;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlRscAutoHelper autoHelper;
    private final CtrlPropsHelper ctrlPropsHelper;
    private final BackupInfoManager backupInfoMgr;
    private final CtrlSatelliteUpdateCaller ctrlStltUpdateCaller;

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
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlRscAutoHelper ctrlRscAutoHelperRef,
        CtrlPropsHelper ctrlPropsHelperRef,
        BackupInfoManager backupInfoMgrRef,
        CtrlSatelliteUpdateCaller ctrlStltUpdateCallerRef
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
        peerAccCtx = peerAccCtxRef;
        lockGuardFactory = lockGuardFactoryRef;
        autoHelper = ctrlRscAutoHelperRef;
        ctrlPropsHelper = ctrlPropsHelperRef;
        backupInfoMgr = backupInfoMgrRef;
        ctrlStltUpdateCaller = ctrlStltUpdateCallerRef;
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
            ResourceDefinition fromRscDfn = ctrlApiDataLoader.loadRscDfn(fromRscName, true);

            SnapshotDefinition fromSnapshotDfn = ctrlApiDataLoader.loadSnapshotDfn(fromRscDfn, fromSnapshotName, true);

            ResourceDefinition toRscDfn = ctrlApiDataLoader.loadRscDfn(toRscName, true);

            if (toRscDfn.getResourceCount() != 0)
            {
                throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_EXISTS_RSC,
                    "Cannot restore to resource definition which already has resources"
                ));
            }

            if (
                isFlagSet(fromSnapshotDfn, SnapshotDefinition.Flags.SHIPPING) ||
                    isFlagSet(fromSnapshotDfn, SnapshotDefinition.Flags.SHIPPING_CLEANUP)
            )
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
                for (Snapshot snapshot : fromSnapshotDfn.getAllSnapshots(peerAccCtx.get()))
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
                    Node node = ctrlApiDataLoader.loadNode(nodeNameStr, true);
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
                    vlm.getProps(peerAccCtx.get()).removeNamespace(
                        ApiConsts.NAMESPC_STLT + "/" + ApiConsts.NAMESPC_EBS
                    );
                }
            }

            autoFlux = autoHelper.manage(new AutoHelperContext(responses, context, toRscDfn)).getFlux();

            ctrlTransactionHelper.commit();

            if (toRscDfn.getVolumeDfnCount(peerAccCtx.get()) == 0)
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
                    toRscDfn.streamResource(peerAccCtx.get())
                        .map(Resource::getUuid)
                        .map(UUID::toString)
                        .collect(Collectors.joining(", ")))
                .build()
            );

            final Flux<ApiCallRc> cleanupFlux = cleanupPropertiesFlux;

            return Flux.<ApiCallRc>just(responses)
                .concatWith(deploymentResponses)
                .concatWith(autoFlux)
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
        @Nullable ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(rscNameRef, false);
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

    private boolean isFlagSet(SnapshotDefinition snapDfn, SnapshotDefinition.Flags... flags)
    {
        boolean flagsSet;
        try
        {
            flagsSet = snapDfn.getFlags().isSet(peerAccCtx.get(), flags);
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "accessing snapshot-definition's flags",
                ApiConsts.FAIL_ACC_DENIED_SNAP_DFN
            );
        }
        return flagsSet;
    }

    private void unsetFlags(ResourceDefinition rscDfn, ResourceDefinition.Flags... flags)
    {
        try
        {
            rscDfn.getFlags().disableFlags(peerAccCtx.get(), flags);
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "setting resource-definition flags",
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
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
            AccessContext peerCtx = peerAccCtx.get();
            for (Resource rsc : restoredResourcesRef)
            {
                Iterator<Volume> iterateVolumes = rsc.iterateVolumes();
                while (iterateVolumes.hasNext())
                {
                    Volume vlm = iterateVolumes.next();
                    Props props = vlm.getProps(peerCtx);
                    props.removeProp(ApiConsts.KEY_VLM_RESTORE_FROM_RESOURCE);
                    props.removeProp(ApiConsts.KEY_VLM_RESTORE_FROM_SNAPSHOT);
                }
            }
            ctrlTransactionHelper.commit();
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "access volume properties",
                ApiConsts.FAIL_ACC_DENIED_VLM
            );
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
        throws AccessDeniedException, InvalidKeyException, InvalidValueException, DatabaseException
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
        rscFlags.enableFlags(peerAccCtx.get(), Resource.Flags.RESTORE_FROM_SNAPSHOT);

        Iterator<VolumeDefinition> toVlmDfnIter = ctrlRscCrtApiHelper.getVlmDfnIterator(toRscDfn);
        while (toVlmDfnIter.hasNext())
        {
            VolumeDefinition toVlmDfn = toVlmDfnIter.next();
            VolumeNumber volumeNumber = toVlmDfn.getVolumeNumber();

            SnapshotVolumeDefinition fromSnapshotVlmDfn =
                fromSnapshotDfn.getSnapshotVolumeDefinition(peerAccCtx.get(), volumeNumber);

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

            long snapshotVolumeSize = fromSnapshotVlmDfn.getVolumeSize(peerAccCtx.get());
            long requiredVolumeSize = toVlmDfn.getVolumeSize(peerAccCtx.get());
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

            Map<String, StorPool> storPool = LayerVlmUtils.getStorPoolMap(snapshot, volumeNumber, peerAccCtx.get());
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
            rsc.getLayerData(peerAccCtx.get()),
            DeviceLayerKind.DRBD
        );
        boolean forceInitSync = false;
        for (AbsRscLayerObject<Resource> layer : drbdLayers)
        {
            DrbdRscData<Resource> drbdRscData = (DrbdRscData<Resource>) layer;
            if (DrbdLayerUtils.isForceInitialSyncSet(peerAccCtx.get(), drbdRscData))
            {
                forceInitSync = true;
                drbdRscData.getFlags().disableFlags(peerAccCtx.get(), DrbdRscFlags.INITIALIZED);
                drbdRscData.getFlags().enableFlags(
                    peerAccCtx.get(),
                    DrbdRscFlags.FROM_BACKUP, DrbdRscFlags.FORCE_NEW_METADATA
                );
            }
        }
        if (forceInitSync)
        {
            rsc.getResourceDefinition().getProps(peerAccCtx.get()).removeProp(InternalApiConsts.PROP_PRIMARY_SET);
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
