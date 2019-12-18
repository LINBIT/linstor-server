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
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.repository.ResourceDefinitionRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.stateflags.StateFlags;
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
import java.util.Iterator;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import reactor.core.publisher.Flux;

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
    private final Provider<AccessContext> peerAccCtx;
    private final LockGuardFactory lockGuardFactory;

    @Inject
    public CtrlRscDfnDeleteApiCallHandler(
        @ApiContext AccessContext apiCtxRef,
        ScopeRunner scopeRunnerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        ResourceDefinitionRepository resourceDefinitionRepositoryRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        ResponseConverter responseConverterRef,
        LockGuardFactory lockGuardFactoryRef,
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
        lockGuardFactory = lockGuardFactoryRef;
        peerAccCtx = peerAccCtxRef;
    }

    @Override
    public Collection<Flux<ApiCallRc>> resourceDefinitionConnected(ResourceDefinition rscDfn, ResponseContext context)
        throws AccessDeniedException
    {
        return rscDfn.getFlags().isSet(apiCtx, ResourceDefinition.Flags.DELETE)
            ?
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
                "Delete resource definition",
                lockGuardFactory.create().write(LockObj.RSC_DFN_MAP).buildDeferred(),
                () -> deleteResourceDefinitionInTransaction(rscNameStr)
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> deleteResourceDefinitionInTransaction(String rscNameStr)
    {
        requireRscDfnMapChangeAccess();

        ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(rscNameStr, false);

        if (rscDfn == null)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.WARN_NOT_FOUND,
                getRscDfnDescription(rscNameStr) + " not found."
            ));
        }

        if (hasSnapshotsPrivileged(rscDfn))
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_EXISTS_SNAPSHOT_DFN,
                "Cannot delete " + getRscDfnDescriptionInline(rscNameStr) + " because it has snapshots."
            ));
        }

        Optional<Resource> rscInUse = null;
        rscInUse = anyResourceInUsePrivileged(rscDfn);
        if (rscInUse.isPresent())
        {
            NodeName nodeName = rscInUse.get().getNode().getName();
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
            markDeleted(rscDfn);

            ctrlTransactionHelper.commit();

            ApiCallRc responses = ApiCallRcImpl.singletonApiCallRc(
                ApiCallRcImpl.entryBuilder(ApiConsts.DELETED, descriptionFirstLetterCaps + " marked for deletion.")
                    .setDetails(descriptionFirstLetterCaps + " UUID is: " + rscDfnUuid).build()
            );

            flux = Flux
                .just(responses)
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
                "Delete starting with diskless resources",
                lockGuardFactory.create().write(LockObj.RSC_DFN_MAP).buildDeferred(),
                () -> deleteDisklessInTransaction(rscName)
            );
    }

    private Flux<ApiCallRc> deleteDisklessInTransaction(ResourceName rscName)
    {
        ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(rscName, false);

        Flux<ApiCallRc> flux;

        if (rscDfn == null)
        {
            flux = Flux.empty();
        }
        else
        {
            boolean hasDisklessResources = false;
            for (Resource rsc : getRscStreamPrivileged(rscDfn).collect(Collectors.toList()))
            {
                if (isDisklessPrivileged(rsc))
                {
                    markDeletedPrivileged(rsc);
                    hasDisklessResources = true;
                }
            }
            ctrlTransactionHelper.commit();

            Flux<ApiCallRc> satelliteUpdateResponses;

            Flux<ApiCallRc> nextStep = deleteRemaining(rscName);
            if (hasDisklessResources)
            {
                satelliteUpdateResponses = ctrlSatelliteUpdateCaller.updateSatellites(rscDfn, nextStep)
                    .transform(updateResponses -> CtrlResponseUtils.combineResponses(
                        updateResponses,
                        rscName,
                        "Notified {0} that diskless resources of {1} are being deleted"
                    ));
            }
            else
            {
                satelliteUpdateResponses = Flux.empty();
            }

            flux = satelliteUpdateResponses
                .concatWith(nextStep)
                .onErrorResume(CtrlResponseUtils.DelayedApiRcException.class, ignored -> Flux.empty());
        }

        return flux;
    }

    private Flux<ApiCallRc> deleteRemaining(ResourceName rscName)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Delete remaining resources",
                lockGuardFactory.create().write(LockObj.RSC_DFN_MAP).buildDeferred(),
                () -> deleteRemainingInTransaction(rscName)
            );
    }

    private Flux<ApiCallRc> deleteRemainingInTransaction(ResourceName rscName)
    {
        ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(rscName, false);

        Flux<ApiCallRc> flux;

        if (rscDfn == null)
        {
            flux = Flux.empty();
        }
        else
        {
            for (Resource rsc : getRscStreamPrivileged(rscDfn).collect(Collectors.toList()))
            {
                if (isDisklessPrivileged(rsc))
                {
                    deletePrivileged(rsc);
                }
                else
                {
                    markDeletedPrivileged(rsc);
                }
            }
            ctrlTransactionHelper.commit();

            Flux<ApiCallRc> nextStep = deleteData(rscName);
            flux = ctrlSatelliteUpdateCaller.updateSatellites(rscDfn, nextStep)
                .transform(updateResponses -> CtrlResponseUtils.combineResponses(
                    updateResponses,
                    rscName,
                    "Resource {1} on {0} deleted"
                ))
                .concatWith(nextStep)
                .onErrorResume(CtrlResponseUtils.DelayedApiRcException.class, ignored -> Flux.empty());
        }

        return flux;
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

    private Stream<Resource> getRscStreamPrivileged(ResourceDefinition rscDfn)
    {
        Stream<Resource> stream;
        try
        {
            stream = rscDfn.streamResource(apiCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        return stream;
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

    private boolean isDisklessPrivileged(Resource rsc)
    {
        boolean isDiskless;
        try
        {
            StateFlags<Flags> stateFlags = rsc.getStateFlags();
            isDiskless = stateFlags.isSet(apiCtx, Resource.Flags.DRBD_DISKLESS) ||
                stateFlags.isSet(apiCtx, Resource.Flags.NVME_INITIATOR);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return isDiskless;
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

    private void markDeletedPrivileged(Resource rsc)
    {
        try
        {
            rsc.markDeleted(apiCtx);
            Iterator<Volume> volumesIterator = rsc.iterateVolumes();
            while (volumesIterator.hasNext())
            {
                Volume vlm = volumesIterator.next();
                vlm.markDeleted(apiCtx);
            }
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

    private void deletePrivileged(Resource rsc)
    {
        try
        {
            rsc.delete(apiCtx);
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
