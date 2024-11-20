package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.BackupInfoManager;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlVlmDfnApiCallHandler.getVlmDfnDescription;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlVlmDfnApiCallHandler.getVlmDfnDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlVlmListApiCallHandler.getVlmDescriptionInline;
import static com.linbit.utils.StringUtils.firstLetterCaps;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import reactor.core.publisher.Flux;

@Singleton
public class CtrlVlmDfnDeleteApiCallHandler implements CtrlSatelliteConnectionListener
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final ScopeRunner scopeRunner;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final ResponseConverter responseConverter;
    private final LockGuardFactory lockguardFactory;
    private final Provider<AccessContext> peerAccCtx;
    private final BackupInfoManager backupInfoMgr;
    private final CtrlResyncAfterHelper ctrlResyncAfterHelper;

    @Inject
    public CtrlVlmDfnDeleteApiCallHandler(
        @ApiContext AccessContext apiCtxRef,
        ScopeRunner scopeRunnerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        ResponseConverter responseConverterRef,
        LockGuardFactory lockguardFactoryRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        BackupInfoManager backupInfoMgrRef,
        CtrlResyncAfterHelper ctrlResyncAfterHelperRef,
        ErrorReporter errorReporterRef
    )
    {
        apiCtx = apiCtxRef;
        scopeRunner = scopeRunnerRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        responseConverter = responseConverterRef;
        lockguardFactory = lockguardFactoryRef;
        peerAccCtx = peerAccCtxRef;
        backupInfoMgr = backupInfoMgrRef;
        ctrlResyncAfterHelper = ctrlResyncAfterHelperRef;
        errorReporter = errorReporterRef;
    }

    @Override
    public Collection<Flux<ApiCallRc>> resourceDefinitionConnected(ResourceDefinition rscDfn, ResponseContext context)
        throws AccessDeniedException
    {
        List<Flux<ApiCallRc>> fluxes = new ArrayList<>();

        Iterator<VolumeDefinition> vlmDfnIter = rscDfn.iterateVolumeDfn(apiCtx);
        while (vlmDfnIter.hasNext())
        {
            VolumeDefinition vlmDfn = vlmDfnIter.next();
            if (
                !rscDfn.getFlags().isSet(apiCtx, ResourceDefinition.Flags.DELETE) &&
                vlmDfn.getFlags().isSet(apiCtx, VolumeDefinition.Flags.DELETE))
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
        VolumeDefinition vlmDfn = ctrlApiDataLoader.loadVlmDfn(rscName, vlmNr, false);

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

        Optional<Resource> rscInUse = anyResourceInUsePrivileged(rscDfn);
        if (rscInUse.isPresent())
        {
            NodeName nodeName = rscInUse.get().getNode().getName();
            throw new ApiRcException(ApiCallRcImpl
                .entryBuilder(
                    ApiConsts.MASK_RSC_DFN | ApiConsts.MASK_DEL | ApiConsts.FAIL_IN_USE,
                    String.format("Resource '%s' on node '%s' is still in use.", rscNameStr, nodeName.displayValue)
                )
                .setCause("Resource is mounted/in use.")
                .setCorrection(String.format("Un-mount resource '%s' on the node '%s'.",
                    rscNameStr,
                    nodeName.displayValue))
                .build()
            );
        }

        // mark volumes to delete or check if all a 'CLEAN'
        Iterator<Volume> itVolumes = getVolumeIteratorPrivileged(vlmDfn);
        while (itVolumes.hasNext())
        {
            Volume vlm = itVolumes.next();
            markDeleted(vlm);
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

        Flux<ApiCallRc> updateResponses = updateSatellites(rscName, vlmNr);

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
        VolumeDefinition vlmDfn = ctrlApiDataLoader.loadVlmDfn(rscName, vlmNr, false);

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
        VolumeDefinition vlmDfn = ctrlApiDataLoader.loadVlmDfn(rscName, vlmNr, false);

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

    private Optional<Resource> anyResourceInUsePrivileged(ResourceDefinition rscDfn)
    {
        Optional<Resource> rscInUse;
        try
        {
            rscInUse = rscDfn.anyResourceInUse(apiCtx);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return rscInUse;
    }

    private Iterator<Volume> getVolumeIteratorPrivileged(VolumeDefinition vlmDfn)
    {
        Iterator<Volume> iterator;
        try
        {
            iterator = vlmDfn.iterateVolumes(apiCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        return iterator;
    }

    private void markDeleted(VolumeDefinition vlmDfn)
    {
        try
        {
            vlmDfn.markDeleted(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "mark " + getVlmDfnDescriptionInline(vlmDfn) + " as deleted",
                ApiConsts.FAIL_ACC_DENIED_VLM_DFN
            );
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
            vlm.markDeleted(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "mark " + getVlmDescriptionInline(vlm) + " as deleted",
                ApiConsts.FAIL_ACC_DENIED_VLM
            );
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
            vlmDfn.delete(apiCtx);
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
