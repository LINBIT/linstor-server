package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceDefinitionData;
import com.linbit.linstor.ResourceDefinitionRepository;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSQLException;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.apicallhandler.response.ResponseUtils;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.locks.LockGuard;
import reactor.core.publisher.Flux;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscDfnApiCallHandler.getRscDfnDescription;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscDfnApiCallHandler.getRscDfnDescriptionInline;
import static com.linbit.utils.StringUtils.firstLetterCaps;

@Singleton
public class CtrlRscDfnDeleteApiCallHandler implements CtrlSatelliteConnectionListener
{
    private final AccessContext apiCtx;
    private final ScopeRunner scopeRunner;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final ResourceDefinitionRepository resourceDefinitionRepository;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final ResponseConverter responseConverter;
    private final ReadWriteLock rscDfnMapLock;
    private final Provider<AccessContext> peerAccCtx;

    @Inject
    public CtrlRscDfnDeleteApiCallHandler(
        @ApiContext AccessContext apiCtxRef,
        ScopeRunner scopeRunnerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        ResourceDefinitionRepository resourceDefinitionRepositoryRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        ResponseConverter responseConverterRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef
    )
    {
        apiCtx = apiCtxRef;
        scopeRunner = scopeRunnerRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        resourceDefinitionRepository = resourceDefinitionRepositoryRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        responseConverter = responseConverterRef;
        rscDfnMapLock = rscDfnMapLockRef;
        peerAccCtx = peerAccCtxRef;
    }

    @Override
    public Collection<Flux<ApiCallRc>> resourceDefinitionConnected(ResourceDefinition rscDfn)
        throws AccessDeniedException
    {
        return rscDfn.getFlags().isSet(apiCtx, ResourceDefinition.RscDfnFlags.DELETE) ?
            Collections.singletonList(deleteDiskless(rscDfn.getName())) :
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
                LockGuard.createDeferred(rscDfnMapLock.writeLock()),
                () -> deleteResourceDefinitionInTransaction(context, rscNameStr)
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> deleteResourceDefinitionInTransaction(ResponseContext context, String rscNameStr)
    {
        requireRscDfnMapChangeAccess();

        ResourceDefinitionData rscDfn = ctrlApiDataLoader.loadRscDfn(rscNameStr, false);

        if (rscDfn == null)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.WARN_NOT_FOUND,
                getRscDfnDescription(rscNameStr) + " not found."
            ));
        }

        if (hasSnapshotsPriveleged(rscDfn))
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_EXISTS_SNAPSHOT_DFN,
                "Cannot delete " + getRscDfnDescriptionInline(rscNameStr) + " because it has snapshots."
            ));
        }

        Optional<Resource> rscInUse = null;
        rscInUse = anyResourceInUsePriveleged(rscDfn);
        if (rscInUse.isPresent())
        {
            NodeName nodeName = rscInUse.get().getAssignedNode().getName();
            throw new ApiRcException(ApiCallRcImpl
                .entryBuilder(
                    ApiConsts.FAIL_IN_USE,
                    String.format("Resource '%s' on node '%s' is still in use.", rscNameStr, nodeName.displayValue)
                )
                .setCause("Resource is mounted/in use.")
                .setCorrection(String.format("Un-mount resource '%s' on the node '%s'.",
                    rscNameStr,
                    nodeName.displayValue))
                .build()
            );
        }

        Flux<ApiCallRc> flux;

        ResourceName rscName = rscDfn.getName();
        UUID rscDfnUuid = rscDfn.getUuid();
        String descriptionFirstLetterCaps = firstLetterCaps(getRscDfnDescriptionInline(rscNameStr));
        if (rscDfn.getResourceCount() > 0)
        {
            ApiCallRcImpl responses = new ApiCallRcImpl();

            markDeleted(rscDfn);

            ctrlTransactionHelper.commit();

            responseConverter.addWithOp(responses, context,
                ApiCallRcImpl.entryBuilder(ApiConsts.DELETED, descriptionFirstLetterCaps + " marked for deletion.")
                    .setDetails(descriptionFirstLetterCaps + " UUID is: " + rscDfnUuid).build()
            );

            flux = Flux
                .<ApiCallRc>just(responses)
                // first delete diskless resources, because DRBD raises an error if all peers with disks are
                // removed from a diskless resource
                .concatWith(deleteDiskless(rscName));
        }
        else
        {
            flux = Flux.just(commitDeleteRscDfnData(rscDfn));
        }

        return flux;
    }

    // Restart from here when connection established and DELETE flag set
    private Flux<ApiCallRc> deleteDiskless(ResourceName rscName)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                LockGuard.createDeferred(rscDfnMapLock.writeLock()),
                () -> deleteDisklessInTransaction(rscName)
            );
    }

    private Flux<ApiCallRc> deleteDisklessInTransaction(ResourceName rscName)
    {
        ResourceDefinitionData rscDfn = ctrlApiDataLoader.loadRscDfn(rscName, false);

        Flux<ApiCallRc> flux;

        if (rscDfn == null)
        {
            flux = Flux.empty();
        }
        else
        {
            boolean hasDisklessResources = false;
            Iterator<Resource> rscIterator = getRscIteratorPrivileged(rscDfn);
            while (rscIterator.hasNext())
            {
                Resource rsc = rscIterator.next();
                if (isDisklessPriveleged(rsc))
                {
                    markDeletedPrivileged(rsc);
                    hasDisklessResources = true;
                }
            }
            ctrlTransactionHelper.commit();

            Flux<ApiCallRc> satelliteUpdateResponses;

            if (hasDisklessResources)
            {
                satelliteUpdateResponses = ctrlSatelliteUpdateCaller.updateSatellites(rscDfn)
                    .transform(updateResponses -> ResponseUtils.translateDeploymentSuccess(
                        updateResponses,
                        "Notified {0} that diskless resources of {1} are being deleted"
                    ));
            }
            else
            {
                satelliteUpdateResponses = Flux.empty();
            }

            flux = satelliteUpdateResponses
                .concatWith(deleteRemaining(rscName))
                .onErrorResume(CtrlSatelliteUpdateCaller.DelayedApiRcException.class, ignored -> Flux.empty());
        }

        return flux;
    }

    private Flux<ApiCallRc> deleteRemaining(ResourceName rscName)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                LockGuard.createDeferred(rscDfnMapLock.writeLock()),
                () -> deleteRemainingInTransaction(rscName)
            );
    }

    private Flux<ApiCallRc> deleteRemainingInTransaction(ResourceName rscName)
    {
        ResourceDefinitionData rscDfn = ctrlApiDataLoader.loadRscDfn(rscName, false);

        Flux<ApiCallRc> flux;

        if (rscDfn == null)
        {
            flux = Flux.empty();
        }
        else
        {
            Iterator<Resource> rscIterator = getRscIteratorPrivileged(rscDfn);
            while (rscIterator.hasNext())
            {
                Resource rsc = rscIterator.next();
                if (isDisklessPriveleged(rsc))
                {
                    deletePriveleged(rsc);
                }
                else
                {
                    markDeletedPrivileged(rsc);
                }
            }
            ctrlTransactionHelper.commit();

            flux = ctrlSatelliteUpdateCaller.updateSatellites(rscDfn)
                .transform(updateResponses -> ResponseUtils.translateDeploymentSuccess(
                    updateResponses,
                    "Resource {1} on {0} deleted"
                ))
                .concatWith(deleteData(rscName))
                .onErrorResume(CtrlSatelliteUpdateCaller.DelayedApiRcException.class, ignored -> Flux.empty());
        }

        return flux;
    }

    private Flux<ApiCallRc> deleteData(ResourceName rscName)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                LockGuard.createDeferred(rscDfnMapLock.writeLock()),
                () -> deleteDataInTransaction(rscName)
            );
    }

    private Flux<ApiCallRc> deleteDataInTransaction(ResourceName rscName)
    {
        ResourceDefinitionData rscDfn = ctrlApiDataLoader.loadRscDfn(rscName, false);

        return rscDfn == null ?
            Flux.empty() :
            Flux.just(commitDeleteRscDfnData(rscDfn));
    }

    private ApiCallRc commitDeleteRscDfnData(ResourceDefinitionData rscDfn)
    {
        ResourceName rscName = rscDfn.getName();
        UUID rscDfnUuid = rscDfn.getUuid();
        String descriptionFirstLetterCaps = firstLetterCaps(getRscDfnDescriptionInline(rscName));

        delete(rscDfn);

        removeResourceDefinitionPriveleged(rscName);
        ctrlTransactionHelper.commit();

        return ApiCallRcImpl.singletonApiCallRc(ApiCallRcImpl
            .entryBuilder(ApiConsts.DELETED, descriptionFirstLetterCaps + " deleted.")
            .setDetails(descriptionFirstLetterCaps + " UUID was: " + rscDfnUuid).build()
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

    private Iterator<Resource> getRscIteratorPrivileged(ResourceDefinitionData rscDfn)
    {
        Iterator<Resource> iterator;
        try
        {
            iterator = rscDfn.iterateResource(apiCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        return iterator;
    }

    private boolean hasSnapshotsPriveleged(ResourceDefinition rscDfn)
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

    private Optional<Resource> anyResourceInUsePriveleged(ResourceDefinition rscDfn)
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

    private boolean isDisklessPriveleged(Resource rsc)
    {
        boolean isDiskless;
        try
        {
            isDiskless = rsc.getStateFlags().isSet(apiCtx, Resource.RscFlags.DISKLESS);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return isDiskless;
    }

    private void markDeleted(ResourceDefinitionData rscDfn)
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
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
    }

    private void markDeletedPrivileged(Resource rsc)
    {
        try
        {
            rsc.markDeleted(apiCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
    }

    private void delete(ResourceDefinitionData rscDfn)
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
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
    }

    private void deletePriveleged(Resource rsc)
    {
        try
        {
            rsc.delete(apiCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
    }

    private void removeResourceDefinitionPriveleged(ResourceName rscName)
    {
        try
        {
            resourceDefinitionRepository.remove(apiCtx, rscName);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }

}
