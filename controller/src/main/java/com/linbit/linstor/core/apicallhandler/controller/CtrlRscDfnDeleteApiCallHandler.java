package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
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
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscDfnApiCallHandler.getRscDfnDescription;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscDfnApiCallHandler.getRscDfnDescriptionInline;
import static com.linbit.utils.StringUtils.firstLetterCaps;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.Collection;
import java.util.Collections;
import java.util.UUID;

import reactor.core.publisher.Flux;

@Singleton
public class CtrlRscDfnDeleteApiCallHandler implements CtrlSatelliteConnectionListener
{
    private final ScopeRunner scopeRunner;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final ResourceDefinitionRepository resourceDefinitionRepository;
    private final ResponseConverter responseConverter;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlRscDfnTruncateApiCallHandler ctrlRscDfnTruncateApiCallHandler;

    @Inject
    public CtrlRscDfnDeleteApiCallHandler(
        ScopeRunner scopeRunnerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        ResourceDefinitionRepository resourceDefinitionRepositoryRef,
        ResponseConverter responseConverterRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlRscDfnTruncateApiCallHandler ctrlRscDfnTruncateApiCallHandlerRef
    )
    {
        scopeRunner = scopeRunnerRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        resourceDefinitionRepository = resourceDefinitionRepositoryRef;
        responseConverter = responseConverterRef;
        lockGuardFactory = lockGuardFactoryRef;
        ctrlRscDfnTruncateApiCallHandler = ctrlRscDfnTruncateApiCallHandlerRef;
    }

    @Override
    public Collection<Flux<ApiCallRc>> resourceDefinitionConnected(ResourceDefinition rscDfn, ResponseContext context)
    {
        return rscDfn.getFlags().isSet(ResourceDefinition.Flags.DELETE) ?
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

    /**
     * Deletes all {@link Resource}s of the given {@link ResourceDefinition} ("truncate") without
     * removing the resource definition itself or any of its snapshots.
     *
     * <p>If {@code deleteEmptyRscDfn} is set, the resource definition is deleted as well when it
     * has neither resources nor snapshots left after the truncate, with the same atomic emptiness
     * check as {@link #deleteResourceDefinitionIfEmpty(ResourceName)}. A resource or snapshot
     * created while the resources are being deleted keeps the resource definition.
     */
    public Flux<ApiCallRc> truncateResourceDefinition(String rscNameStr, boolean deleteEmptyRscDfn)
    {
        ResponseContext context = CtrlRscDfnApiCallHandler.makeResourceDefinitionContext(
            ApiOperation.makeDeleteOperation(),
            rscNameStr
        );

        return scopeRunner
            .fluxInTransactionalScope(
                "Truncate resource definition",
                lockGuardFactory.create().write(LockObj.NODES_MAP, LockObj.RSC_DFN_MAP).buildDeferred(),
                () -> truncateResourceDefinitionInTransaction(rscNameStr, deleteEmptyRscDfn)
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> truncateResourceDefinitionInTransaction(
        String rscNameRef,
        boolean deleteEmptyRscDfn
    )
    {
        requireRscDfnMapChangeAccess();

        @Nullable ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfnOrNull(rscNameRef);

        if (rscDfn == null)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.WARN_NOT_FOUND,
                getRscDfnDescription(rscNameRef) + " not found."
            ));
        }

        Flux<ApiCallRc> flux = ctrlRscDfnTruncateApiCallHandler.truncateRscDfnInTransaction(rscDfn.getName(), false)
            .onErrorResume(CtrlResponseUtils.DelayedApiRcException.class, ignored -> Flux.empty());
        if (deleteEmptyRscDfn)
        {
            flux = flux.concatWith(deleteResourceDefinitionIfEmpty(rscDfn.getName()));
        }
        return flux;
    }

    /**
     * Deletes the given {@link ResourceDefinition} only if it currently has neither resources nor
     * snapshots. If the resource definition still has resources or snapshots (or no longer exists),
     * this method does nothing.
     *
     * <p>The emptiness check and the deletion happen atomically under the resource-definition write
     * lock, so no resource or snapshot can be created between the check and the deletion.
     */
    public Flux<ApiCallRc> deleteResourceDefinitionIfEmpty(ResourceName rscName)
    {
        ResponseContext context = CtrlRscDfnApiCallHandler.makeResourceDefinitionContext(
            ApiOperation.makeDeleteOperation(),
            rscName.displayValue
        );

        return scopeRunner
            .fluxInTransactionalScope(
                "Delete resource definition if empty",
                lockGuardFactory.create().write(LockObj.NODES_MAP, LockObj.RSC_DFN_MAP).buildDeferred(),
                () -> deleteResourceDefinitionIfEmptyInTransaction(rscName)
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> deleteResourceDefinitionIfEmptyInTransaction(ResourceName rscName)
    {
        @Nullable ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfnOrNull(rscName);

        Flux<ApiCallRc> flux;
        if (rscDfn == null || rscDfn.isDeleted() || rscDfn.getResourceCount() > 0 || hasSnapshotsPrivileged(rscDfn))
        {
            // The resource definition no longer exists or still has resources or snapshots: keep it.
            flux = Flux.empty();
        }
        else
        {
            flux = deleteResourceDefinitionInTransaction(rscName.displayValue);
        }
        return flux;
    }

    // Restart from here when connection established and DELETE flag set
    private Flux<ApiCallRc> deleteResourceDefinitionInTransaction(String rscNameRef)
    {
        requireRscDfnMapChangeAccess();

        @Nullable ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfnOrNull(rscNameRef);

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
            rscDfn.markDeleted();
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
        @Nullable ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfnOrNull(rscName);

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
    }

    private boolean hasSnapshotsPrivileged(ResourceDefinition rscDfn)
    {
        boolean hasSnapshots;
        hasSnapshots = !rscDfn.getSnapshotDfns().isEmpty();
        return hasSnapshots;
    }


    private void delete(ResourceDefinition rscDfn)
    {
        try
        {
            rscDfn.delete();
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
    }

    private void removeResourceDefinitionPriveleged(ResourceName rscName, byte[] externalName)
    {
        resourceDefinitionRepository.remove(rscName, externalName);
    }

}
