package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.SharedResourceManager;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscAutoHelper.AutoHelperContext;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscAutoHelper.AutoHelperResult;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.utils.layer.LayerVlmUtils;
import com.linbit.locks.LockGuard;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscApiCallHandler.getRscDescription;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscApiCallHandler.getRscDescriptionInline;
import static com.linbit.utils.StringUtils.firstLetterCaps;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.locks.ReadWriteLock;

import reactor.core.publisher.Flux;

@Singleton
public class CtrlRscDeleteApiCallHandler implements CtrlSatelliteConnectionListener
{
    private final AccessContext apiCtx;
    private final ScopeRunner scopeRunner;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final CtrlRscDeleteApiHelper ctrlRscDeleteApiHelper;
    private final ResponseConverter responseConverter;
    private final ReadWriteLock nodesMapLock;
    private final ReadWriteLock rscDfnMapLock;
    private final Provider<AccessContext> peerAccCtx;
    private final CtrlRscAutoHelper autoHelper;
    private final CtrlSnapshotShippingAbortHandler snapShipAbortHandler;
    private final SharedResourceManager sharedRscMgr;

    @Inject
    public CtrlRscDeleteApiCallHandler(
        @ApiContext AccessContext apiCtxRef,
        ScopeRunner scopeRunnerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        CtrlRscDeleteApiHelper ctrlRscDeleteApiHelperRef,
        ResponseConverter responseConverterRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        CtrlRscAutoHelper autoHelperRef,
        CtrlSnapshotShippingAbortHandler snapShipAbortHandlerRef,
        SharedResourceManager sharedRscMgrRef
    )
    {
        apiCtx = apiCtxRef;
        scopeRunner = scopeRunnerRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        ctrlRscDeleteApiHelper = ctrlRscDeleteApiHelperRef;
        responseConverter = responseConverterRef;
        nodesMapLock = nodesMapLockRef;
        rscDfnMapLock = rscDfnMapLockRef;
        peerAccCtx = peerAccCtxRef;
        autoHelper = autoHelperRef;
        snapShipAbortHandler = snapShipAbortHandlerRef;
        sharedRscMgr = sharedRscMgrRef;
    }

    @Override
    public Collection<Flux<ApiCallRc>> resourceDefinitionConnected(
        ResourceDefinition rscDfn,
        ResponseContext context
    )
        throws AccessDeniedException
    {
        List<Flux<ApiCallRc>> fluxes = new ArrayList<>();
        Set<NodeName> nodeNamesToDelete = new TreeSet<>();

        Iterator<Resource> rscIter = rscDfn.iterateResource(apiCtx);
        while (rscIter.hasNext())
        {
            Resource rsc = rscIter.next();
            if (
                !rsc.getNode().getFlags().isSet(apiCtx, Node.Flags.DELETE) &&
                !rscDfn.getFlags().isSet(apiCtx, ResourceDefinition.Flags.DELETE) &&
                rsc.getStateFlags().isSet(apiCtx, Resource.Flags.DELETE)
            )
            {
                nodeNamesToDelete.add(rsc.getNode().getName());
            }
        }

        if (!nodeNamesToDelete.isEmpty())
        {
            fluxes.add(
                ctrlRscDeleteApiHelper.updateSatellitesForResourceDelete(
                    nodeNamesToDelete, rscDfn.getName()
                )
            );
        }

        return fluxes;
    }

    /**
     * Deletes a {@link Resource}.
     * <p>
     * The {@link Resource} is only deleted once the corresponding satellite confirmed
     * that it has undeployed (deleted) the {@link Resource}
     */
    public Flux<ApiCallRc> deleteResource(String nodeNameStr, String rscNameStr)
    {
        ResponseContext context = CtrlRscApiCallHandler.makeRscContext(
            ApiOperation.makeDeleteOperation(),
            nodeNameStr,
            rscNameStr
        );

        return scopeRunner
            .fluxInTransactionalScope(
                "Delete resource",
                LockGuard.createDeferred(nodesMapLock.writeLock(), rscDfnMapLock.writeLock()),
                () -> deleteResourceInTransaction(nodeNameStr, rscNameStr, context)
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> deleteResourceInTransaction(String nodeNameStr, String rscNameStr, ResponseContext context)
    {
        Resource rsc = ctrlApiDataLoader.loadRsc(nodeNameStr, rscNameStr, false);

        if (rsc == null)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.WARN_NOT_FOUND,
                getRscDescription(nodeNameStr, rscNameStr) + " not found."
            ));
        }

        Set<NodeName> nodeNamesToDelete = new TreeSet<>();
        NodeName nodeName = rsc.getNode().getName();

        ctrlRscDeleteApiHelper.ensureNotInUse(rsc);
        ctrlRscDeleteApiHelper.ensureNotLastDisk(rsc);

        failIfDependentSnapshot(rsc);

        activateIfLast(rsc);

        ctrlRscDeleteApiHelper.markDeletedWithVolumes(rsc);
        nodeNamesToDelete.add(nodeName);

        ApiCallRcImpl responses = new ApiCallRcImpl();
        AutoHelperResult autoResult = autoHelper.manage(
            new AutoHelperContext(
                responses,
                context,
                rsc.getDefinition()
            )
        );

        Flux<ApiCallRc> abortSnapShipFlux = snapShipAbortHandler.abortSnapshotShippingPrivileged(rsc.getDefinition());

        ctrlTransactionHelper.commit();


        Flux<ApiCallRc> flux;
        if (!autoResult.isPreventUpdateSatellitesForResourceDelete())
        {
            String descriptionFirstLetterCaps = firstLetterCaps(getRscDescription(rsc));
            responses.addEntries(
                ApiCallRcImpl.singletonApiCallRc(
                    ApiCallRcImpl
                        .entryBuilder(
                            ApiConsts.DELETED,
                            descriptionFirstLetterCaps + " marked for deletion."
                        )
                        .setDetails(descriptionFirstLetterCaps + " UUID is: " + rsc.getUuid())
                        .build()
                )
            );
            flux = Flux.just(responses);

            ResourceName rscName = rsc.getDefinition().getName();
            flux = flux.concatWith(
                ctrlRscDeleteApiHelper.updateSatellitesForResourceDelete(nodeNamesToDelete, rscName)
            );
        }
        else
        {
            flux = Flux.just(responses);
        }
        flux = flux
            .concatWith(abortSnapShipFlux)
            .concatWith(autoResult.getFlux());

        return flux;
    }

    private void activateIfLast(Resource rsc)
    {
        boolean found = false;
        TreeSet<Resource> sharedResources = sharedRscMgr.getSharedResources(rsc);
        for (Resource sharedRsc : sharedResources)
        {
            if (!isFlagSet(sharedRsc, Resource.Flags.INACTIVE_PERMANENTLY))
            {
                // at least one active or inactive resource exists.
                found = true;
                break;
            }

        }
        if (!found)
        {
            // this is the last shared resource, we have to make it active so that the lowest
            // layer can cleanup the volumes
            if (isFlagSet(rsc, Resource.Flags.INACTIVE))
            {
                unsetFlag(rsc, Resource.Flags.INACTIVE);
            }
        }
    }

    private boolean isFlagSet(Resource rsc, Resource.Flags... flags)
    {
        boolean isSet;
        try
        {
            isSet = rsc.getStateFlags().isSet(apiCtx, flags);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }

        return isSet;
    }

    private void unsetFlag(Resource rsc, Resource.Flags... flags)
    {
        try
        {
            rsc.getStateFlags().disableFlags(apiCtx, flags);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
    }

    private void ensureNotLastDisk(ResourceName rscName, Resource rsc)
    {
        try
        {
            AccessContext accCtx = peerAccCtx.get();
            boolean isDiskless = rsc.isDrbdDiskless(accCtx) || rsc.isNvmeInitiator(accCtx);
            if (
                !isDiskless &&
                rsc.getDefinition().hasDisklessNotDeleting(accCtx) &&
                rsc.getDefinition().diskfullCount(accCtx) == 1)
            {
                throw new ApiRcException(ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.FAIL_IN_USE,
                        String.format(
                            "Last resource of '%s' with disk still has diskless resources attached.", rscName)
                    )
                    .setCause("Resource still has diskless users.")
                    .setCorrection("Before deleting this resource, delete the diskless resources attached to it.")
                    .build()
                );
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "check whether is last with disk " + getRscDescriptionInline(rsc),
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
    }

    private void failIfDependentSnapshot(Resource rsc)
    {
        try
        {
            for (SnapshotDefinition snapshotDfn : rsc.getDefinition().getSnapshotDfns(peerAccCtx.get()))
            {
                Snapshot snapshot = snapshotDfn.getSnapshot(peerAccCtx.get(), rsc.getNode().getName());
                if (snapshot != null)
                {
                    Iterator<SnapshotVolume> snapVlmIt = snapshot.iterateVolumes();
                    while (snapVlmIt.hasNext())
                    {
                        SnapshotVolume snapshotVlm = snapVlmIt.next();
                        Set<StorPool> storPoolSet = LayerVlmUtils.getStorPoolSet(snapshotVlm, apiCtx, true);
                        for (StorPool storPool : storPoolSet)
                        {
                            if (storPool.getDeviceProviderKind().isSnapshotDependent())
                            {
                                throw new ApiRcException(
                                    ApiCallRcImpl.simpleEntry(
                                        ApiConsts.FAIL_EXISTS_SNAPSHOT,
                                        "Resource '" + rsc.getDefinition().getName() +
                                            "' cannot be deleted because volume " +
                                            snapshotVlm.getVolumeNumber() + " has dependent snapshot '" +
                                            snapshot.getSnapshotName() + "'"
                                    )
                                );
                            }
                        }
                    }
                }
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "check for dependent snapshots of " + getRscDescriptionInline(rsc),
                ApiConsts.FAIL_ACC_DENIED_SNAPSHOT_DFN
            );
        }
    }
}
