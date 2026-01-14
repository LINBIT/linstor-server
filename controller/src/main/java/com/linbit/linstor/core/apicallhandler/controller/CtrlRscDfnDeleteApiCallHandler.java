package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.repository.ResourceDefinitionRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscDfnApiCallHandler.getRscDfnDescription;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscDfnApiCallHandler.getRscDfnDescriptionInline;
import static com.linbit.utils.StringUtils.firstLetterCaps;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Collection;
import java.util.Collections;
import java.util.UUID;

import reactor.core.publisher.Flux;

@Singleton
public class CtrlRscDfnDeleteApiCallHandler implements CtrlSatelliteConnectionListener
{
    private final AccessContext apiCtx;
    private final ScopeRunner scopeRunner;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final ResourceDefinitionRepository resourceDefinitionRepository;
    private final ResponseConverter responseConverter;
    private final Provider<AccessContext> peerAccCtx;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlRscDfnTruncateApiCallHandler ctrlRscDfnTruncateApiCallHandler;

    @Inject
    public CtrlRscDfnDeleteApiCallHandler(
        @ApiContext AccessContext apiCtxRef,
        ScopeRunner scopeRunnerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        ResourceDefinitionRepository resourceDefinitionRepositoryRef,
        ResponseConverter responseConverterRef,
        LockGuardFactory lockGuardFactoryRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        CtrlRscDfnTruncateApiCallHandler ctrlRscDfnTruncateApiCallHandlerRef
    )
    {
        apiCtx = apiCtxRef;
        scopeRunner = scopeRunnerRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        resourceDefinitionRepository = resourceDefinitionRepositoryRef;
        responseConverter = responseConverterRef;
        lockGuardFactory = lockGuardFactoryRef;
        peerAccCtx = peerAccCtxRef;
        ctrlRscDfnTruncateApiCallHandler = ctrlRscDfnTruncateApiCallHandlerRef;
    }

    @Override
    public Collection<Flux<ApiCallRc>> resourceDefinitionConnected(ResourceDefinition rscDfn, ResponseContext context)
        throws AccessDeniedException
    {
        return rscDfn.getFlags().isSet(apiCtx, ResourceDefinition.Flags.DELETE) ?
            Collections.singletonList(deleteResourceDefinition(rscDfn.getName().displayValue)) :
            Collections.emptyList();
    }

    /**
     * Marks a {@link ResourceDefinition} for deletion.
     *
     * It will only be removed when all satellites confirm the deletion of the corresponding
     * {@link Resource}s.
     */
    public Flux<ApiCallRc> deleteResourceDefinition(String rscNameStr)
    {
        ResponseContext context = CtrlRscDfnApiCallHandler.makeResourceDefinitionContext(
            ApiOperation.makeDeleteOperation(),
            rscNameStr
        );

        return scopeRunner
            .fluxInTransactionalScope(
                "Delete resource definition",
                lockGuardFactory.create().write(LockObj.RSC_DFN_MAP).buildDeferred(),
                () -> deleteResourceDefinitionInTransaction(rscNameStr)
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    // Restart from here when connection established and DELETE flag set
    private Flux<ApiCallRc> deleteResourceDefinitionInTransaction(String rscNameRef)
    {
        requireRscDfnMapChangeAccess();

        ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(rscNameRef, false);

        if (rscDfn == null)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.WARN_NOT_FOUND,
                getRscDfnDescription(rscNameRef) + " not found."
            ));
        }

        // fail fast
        ensureNoSnapDfns(rscDfn);
        ctrlRscDfnTruncateApiCallHandler.ensureNoRscInUse(rscDfn);
        markDeleted(rscDfn);

        ctrlTransactionHelper.commit();

        return ctrlRscDfnTruncateApiCallHandler.truncateRscDfnInTransaction(rscDfn.getName(), false)
            .concatWith(deleteData(rscDfn.getName()))
            .onErrorResume(CtrlResponseUtils.DelayedApiRcException.class, ignored -> Flux.empty());
    }

    private void ensureNoSnapDfns(ResourceDefinition rscDfn)
    {
        if (hasSnapshotsPrivileged(rscDfn))
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_EXISTS_SNAPSHOT_DFN,
                    "Cannot delete " + getRscDfnDescriptionInline(rscDfn) + " because it has snapshots."
                )
            );
        }
    }

    private void markDeleted(ResourceDefinition rscDfn)
    {
        try
        {
            rscDfn.markDeleted(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "mark " + getRscDfnDescriptionInline(rscDfn) + " as deleted",
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
    }

    private Flux<ApiCallRc> deleteData(ResourceName rscName)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Delete resource definition data",
                lockGuardFactory.create().write(LockObj.RSC_DFN_MAP).buildDeferred(),
                () -> deleteDataInTransaction(rscName)
            );
    }

    private Flux<ApiCallRc> deleteDataInTransaction(ResourceName rscName)
    {
        ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(rscName, false);

        return rscDfn == null ?
            Flux.empty() :
            Flux.just(commitDeleteRscDfnData(rscDfn));
    }

    private ApiCallRc commitDeleteRscDfnData(ResourceDefinition rscDfn)
    {
        ResourceName rscName = rscDfn.getName();
        byte[] externalName = rscDfn.getExternalName();
        UUID rscDfnUuid = rscDfn.getUuid();
        String descriptionFirstLetterCaps = firstLetterCaps(getRscDfnDescriptionInline(rscName));

        delete(rscDfn);
        removeResourceDefinitionPriveleged(rscName, externalName);
        ctrlTransactionHelper.commit();

        return ApiCallRcImpl.singletonApiCallRc(ApiCallRcImpl
            .entryBuilder(
                ApiConsts.DELETED,
                descriptionFirstLetterCaps + " deleted."
            )
            .setDetails(descriptionFirstLetterCaps + " UUID was: " + rscDfnUuid)
            .build()
        );
    }

    private void requireRscDfnMapChangeAccess()
    {
        try
        {
            resourceDefinitionRepository.requireAccess(
                peerAccCtx.get(),
                AccessType.CHANGE
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "change any resource definitions",
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }
    }

    private boolean hasSnapshotsPrivileged(ResourceDefinition rscDfn)
    {
        boolean hasSnapshots;
        try
        {
            hasSnapshots = !rscDfn.getSnapshotDfns(apiCtx).isEmpty();
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return hasSnapshots;
    }


    private void delete(ResourceDefinition rscDfn)
    {
        try
        {
            rscDfn.delete(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "delete " + getRscDfnDescriptionInline(rscDfn),
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
    }

    private void removeResourceDefinitionPriveleged(ResourceName rscName, byte[] externalName)
    {
        try
        {
            resourceDefinitionRepository.remove(apiCtx, rscName, externalName);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }

}
