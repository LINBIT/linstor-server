package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.BackupInfoManager;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller.NotConnectedHandler;
import com.linbit.linstor.core.apicallhandler.controller.utils.ZfsChecks;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.apicallhandler.response.ResponseUtils;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.utils.layer.LayerVlmUtils;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlVlmDfnApiCallHandler.getVlmDfnDescription;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlVlmDfnApiCallHandler.getVlmDfnDescriptionInline;
import static com.linbit.utils.StringUtils.firstLetterCaps;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import reactor.core.publisher.Flux;

@Singleton
public class CtrlVlmDfnDeleteApiCallHandler implements CtrlSatelliteConnectionListener
{
    private final ErrorReporter errorReporter;
    private final ScopeRunner scopeRunner;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final ResponseConverter responseConverter;
    private final LockGuardFactory lockguardFactory;
    private final BackupInfoManager backupInfoMgr;
    private final CtrlResyncAfterHelper ctrlResyncAfterHelper;
    private final ZfsChecks zfsChecks;

    @Inject
    public CtrlVlmDfnDeleteApiCallHandler(
        ScopeRunner scopeRunnerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        ResponseConverter responseConverterRef,
        LockGuardFactory lockguardFactoryRef,
        BackupInfoManager backupInfoMgrRef,
        CtrlResyncAfterHelper ctrlResyncAfterHelperRef,
        ErrorReporter errorReporterRef,
        ZfsChecks zfsChecksRef
    )
    {
        scopeRunner = scopeRunnerRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        responseConverter = responseConverterRef;
        lockguardFactory = lockguardFactoryRef;
        backupInfoMgr = backupInfoMgrRef;
        ctrlResyncAfterHelper = ctrlResyncAfterHelperRef;
        errorReporter = errorReporterRef;
        zfsChecks = zfsChecksRef;
    }

    @Override
    public Collection<Flux<ApiCallRc>> resourceDefinitionConnected(ResourceDefinition rscDfn, ResponseContext context)
    {
        List<Flux<ApiCallRc>> fluxes = new ArrayList<>();

        Iterator<VolumeDefinition> vlmDfnIter = rscDfn.iterateVolumeDfn();
        while (vlmDfnIter.hasNext())
        {
            VolumeDefinition vlmDfn = vlmDfnIter.next();
            if (
                !rscDfn.getFlags().isSet(ResourceDefinition.Flags.DELETE) &&
                vlmDfn.getFlags().isSet(VolumeDefinition.Flags.DELETE))
            {
                fluxes.add(updateSatellites(rscDfn.getName(), vlmDfn.getVolumeNumber()));
            }
        }

        return fluxes;
    }

    /**
     * Deletes a {@link VolumeDefinition} for a given {@link ResourceDefinition} and volume nr.
     */
    public Flux<ApiCallRc> deleteVolumeDefinition(String rscNameStr, int vlmNrInt)
    {
        ResponseContext context = CtrlVlmDfnApiCallHandler.makeVlmDfnContext(
            ApiOperation.makeDeleteOperation(),
            rscNameStr,
            vlmNrInt
        );

        return scopeRunner
            .fluxInTransactionalScope(
                "Delete volume definition",
                lockguardFactory.buildDeferred(LockType.WRITE, LockObj.RSC_DFN_MAP),
                () -> deleteVolumeDefinitionInTransaction(rscNameStr, vlmNrInt)
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> deleteVolumeDefinitionInTransaction(String rscNameStr, int vlmNrInt)
    {
        ResourceName rscName = LinstorParsingUtils.asRscName(rscNameStr);
        VolumeNumber vlmNr = LinstorParsingUtils.asVlmNr(vlmNrInt);
        @Nullable VolumeDefinition vlmDfn = ctrlApiDataLoader.loadVlmDfnOrNull(rscName, vlmNr);

        if (vlmDfn == null)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.WARN_NOT_FOUND,
                getVlmDfnDescription(rscNameStr, vlmNrInt) + " not found."
            ));
        }

        UUID vlmDfnUuid = vlmDfn.getUuid();
        ResourceDefinition rscDfn = vlmDfn.getResourceDefinition();
        if (backupInfoMgr.restoreContainsRscDfn(rscDfn))
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_IN_USE,
                    rscNameStr + " is currently being restored from a backup. " +
                        "Please wait until the restore is finished"
                )
            );
        }

        Optional<Resource> rscInUse = rscDfn.anyResourceInUseOrOpen();
        if (rscInUse.isPresent())
        {
            NodeName nodeName = rscInUse.get().getNode().getName();
            throw new ApiRcException(
                CtrlRscInUseHelper.addInUseDetails(
                    ApiCallRcImpl.entryBuilder(
                        ApiConsts.MASK_RSC_DFN | ApiConsts.MASK_DEL | ApiConsts.FAIL_IN_USE,
                        String.format("Resource '%s' on node '%s' is still in use.", rscNameStr, nodeName.displayValue)
                    ),
                    nodeName
                ).build()
            );
        }

        zfsChecks.ensureNoDependentSnapshots(vlmDfn);

        // mark volumes to delete or check if all a 'CLEAN'
        Iterator<Volume> itVolumes = getVolumeIteratorPrivileged(vlmDfn);
        Set<SnapshotDefinition> handleZfsRenameIfNeeded = new HashSet<>();
        while (itVolumes.hasNext())
        {
            Volume vlm = itVolumes.next();
            markDeleted(vlm);

            handleZfsRenameIfNeeded.addAll(
                CtrlRscDeleteApiCallHandler.handleZfsRenameIfNeeded(
                    vlm.getAbsResource()
                )
            );
        }

        markDeleted(vlmDfn);
        ctrlTransactionHelper.commit();

        ApiCallRc responses = ApiCallRcImpl.singletonApiCallRc(ApiCallRcImpl
            .entryBuilder(
                ApiConsts.DELETED,
                firstLetterCaps(getVlmDfnDescriptionInline(rscDfn, vlmNr)) + " marked for deletion."
            )
            .setDetails(firstLetterCaps(getVlmDfnDescriptionInline(rscDfn, vlmNr)) + " UUID is: " + vlmDfnUuid)
            .build()
        );

        Flux<ApiCallRc> updateResponses = Flux.empty();
        NotConnectedHandler notConnectedHandler = nodeName -> Flux.error(
            new ApiRcException(ResponseUtils.makeNotConnectedWarning(nodeName))
        );
        // TODO re really need an atomic updater...
        for (SnapshotDefinition snapDfnToUpdate : handleZfsRenameIfNeeded)
        {
            updateResponses = updateResponses.concatWith(
                ctrlSatelliteUpdateCaller.updateSatellites(snapDfnToUpdate, notConnectedHandler)
                    .transform(
                        updateResponse -> CtrlResponseUtils.combineResponses(
                            errorReporter,
                            updateResponse,
                            rscName,
                            "SnapshotDefinition {1} on {0} updated"
                        )
                    )
            );
        }
        updateResponses = updateResponses.concatWith(updateSatellites(rscName, vlmNr));

        return Flux
            .just(responses)
            .concatWith(ctrlResyncAfterHelper.fluxManage())
            .concatWith(updateResponses);
    }

    // Restart from here when connection established and DELETE flag set
    private Flux<ApiCallRc> updateSatellites(ResourceName rscName, VolumeNumber vlmNr)
    {
        return scopeRunner
            .fluxInTransactionlessScope(
                "Update for volume definition deletion",
                lockguardFactory.buildDeferred(LockType.READ, LockObj.RSC_DFN_MAP),
                () -> updateSatellitesInScope(rscName, vlmNr)
            );
    }

    private Flux<ApiCallRc> updateSatellitesInScope(ResourceName rscName, VolumeNumber vlmNr)
    {
        @Nullable VolumeDefinition vlmDfn = ctrlApiDataLoader.loadVlmDfnOrNull(rscName, vlmNr);

        Flux<ApiCallRc> flux;

        if (vlmDfn == null)
        {
            flux = Flux.empty();
        }
        else
        {
            Flux<ApiCallRc> deleteDataFlux = deleteData(rscName, vlmNr);
            flux = ctrlSatelliteUpdateCaller.updateSatellites(vlmDfn.getResourceDefinition(), deleteDataFlux)
                .transform(updateResponses -> CtrlResponseUtils.combineResponses(
                    errorReporter,
                    updateResponses,
                    rscName,
                    "Deleted volume " + vlmNr + " of {1} on {0}"
                ))
                .concatWith(deleteDataFlux)
                .onErrorResume(CtrlResponseUtils.DelayedApiRcException.class, ignored -> Flux.empty());
        }

        return flux;
    }

    private Flux<ApiCallRc> deleteData(ResourceName rscName, VolumeNumber vlmNr)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Delete volume definition data",
                lockguardFactory.buildDeferred(LockType.WRITE, LockObj.RSC_DFN_MAP),
                () -> deleteDataInTransaction(rscName, vlmNr)
            );
    }

    private Flux<ApiCallRc> deleteDataInTransaction(ResourceName rscName, VolumeNumber vlmNr)
    {
        @Nullable VolumeDefinition vlmDfn = ctrlApiDataLoader.loadVlmDfnOrNull(rscName, vlmNr);

        Flux<ApiCallRc> flux;

        if (vlmDfn == null)
        {
            flux = Flux.empty();
        }
        else
        {
            UUID vlmDfnUuid = vlmDfn.getUuid();
            String descriptionFirstLetterCaps = firstLetterCaps(getVlmDfnDescriptionInline(vlmDfn));

            deletePrivileged(vlmDfn);

            ctrlTransactionHelper.commit();

            flux = Flux.just(ApiCallRcImpl.singletonApiCallRc(ApiCallRcImpl
                .entryBuilder(ApiConsts.DELETED, descriptionFirstLetterCaps + " deleted.")
                .setDetails(descriptionFirstLetterCaps + " UUID was: " + vlmDfnUuid)
                .build()
            ));
        }

        return flux;
    }

    @SuppressWarnings("UnusedMethod") // https://gitlab.at.linbit.com/linstor/linstor-server/-/issues/1317
    private void failIfDependentSnapshot(VolumeDefinition vlmDfn)
    {
        ResourceDefinition rscDfn = vlmDfn.getResourceDefinition();
        for (SnapshotDefinition snapshotDfn : rscDfn.getSnapshotDfns())
        {
            for (Snapshot snapshot : snapshotDfn.getAllSnapshots())
            {
                @Nullable SnapshotVolume snapshotVlm = snapshot.getVolume(vlmDfn.getVolumeNumber());
                if (snapshotVlm != null)
                {
                    Map<String, StorPool> storPoolMap = LayerVlmUtils.getStorPoolMap(snapshotVlm);
                    for (StorPool storPool : storPoolMap.values())
                    {
                        if (storPool.getDeviceProviderKind().isSnapshotDependent())
                        {
                            throw new ApiRcException(
                                ApiCallRcImpl.simpleEntry(
                                    ApiConsts.FAIL_EXISTS_SNAPSHOT,
                                    "Volume definition " + vlmDfn.getVolumeNumber() + " of '" + rscDfn.getName() +
                                        "' cannot be deleted because dependent snapshot '" +
                                        snapshot.getSnapshotName() +
                                        "' is present on node '" + snapshot.getNodeName() + "'"
                                )
                                    .setSkipErrorReport(true)
                            );
                        }
                    }
                }
            }
        }
    }

    private Iterator<Volume> getVolumeIteratorPrivileged(VolumeDefinition vlmDfn)
    {
        Iterator<Volume> iterator;
        iterator = vlmDfn.iterateVolumes();
        return iterator;
    }

    private void markDeleted(VolumeDefinition vlmDfn)
    {
        try
        {
            vlmDfn.markDeleted();
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
    }

    private void markDeleted(Volume vlm)
    {
        try
        {
            vlm.markDeleted();
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
    }

    private void deletePrivileged(VolumeDefinition vlmDfn)
    {
        try
        {
            vlmDfn.delete();
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
    }
}
