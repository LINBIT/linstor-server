package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.SharedResourceManager;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscAutoHelper.AutoHelperContext;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscAutoHelper.AutoHelperResult;
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
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.utils.LayerUtils;
import com.linbit.linstor.utils.layer.LayerVlmUtils;
import com.linbit.locks.LockGuard;
import com.linbit.utils.TimeUtils;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscApiCallHandler.getRscDescription;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscApiCallHandler.getRscDescriptionInline;
import static com.linbit.utils.StringUtils.firstLetterCaps;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.locks.ReadWriteLock;

import reactor.core.publisher.Flux;

@Singleton
public class CtrlRscDeleteApiCallHandler implements CtrlSatelliteConnectionListener
{
    private static final String PROP_KEY_ZFS_RENAME_SUFFIX = InternalApiConsts.NAMESPC_INTERNAL + "/" +
        ApiConsts.NAMESPC_STORAGE_DRIVER + "/" + InternalApiConsts.KEY_ZFS_RENAME_SUFFIX;

    private final ErrorReporter errorReporter;
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
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final CtrlRscActivateApiCallHandler ctrlRscActivateApiCallHandler;
    private final Provider<CtrlRscDfnApiCallHandler> ctrlRscDfnApiCallHandler;

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
        SharedResourceManager sharedRscMgrRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        CtrlRscActivateApiCallHandler ctrlRscActivateApiCallHandlerRef,
        Provider<CtrlRscDfnApiCallHandler> ctrlRscDfnApiCallHandlerRef,
        ErrorReporter errorReporterRef
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
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        ctrlRscActivateApiCallHandler = ctrlRscActivateApiCallHandlerRef;
        ctrlRscDfnApiCallHandler = ctrlRscDfnApiCallHandlerRef;
        errorReporter = errorReporterRef;
    }

    @Override
    public Collection<Flux<ApiCallRc>> resourceDefinitionConnected(
        ResourceDefinition rscDfn,
        ResponseContext contextRef
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
                    contextRef,
                    nodeNamesToDelete,
                    rscDfn.getName()
                )
            );
        }

        return fluxes;
    }

    public Flux<ApiCallRc> deleteResource(String nodeNameStr, String rscNameStr)
    {
        return deleteResource(nodeNameStr, rscNameStr, false);
    }
    /**
     * Deletes a {@link Resource}.
     * <p>
     * The {@link Resource} is only deleted once the corresponding satellite confirmed
     * that it has undeployed (deleted) the {@link Resource}
     * @param keepTiebreakerRef
     */
    public Flux<ApiCallRc> deleteResource(String nodeNameStr, String rscNameStr, boolean keepTiebreakerRef)
    {
        ResponseContext context = CtrlRscApiCallHandler.makeRscContext(
            ApiOperation.makeDeleteOperation(),
            nodeNameStr,
            rscNameStr
        );

        return scopeRunner
            .fluxInTransactionalScope(
                "Activating resource if necessary before deletion",
                LockGuard.createDeferred(nodesMapLock.writeLock(), rscDfnMapLock.writeLock()),
                () -> activateRscIfLastInTransaction(nodeNameStr, rscNameStr, keepTiebreakerRef, context)
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> activateRscIfLastInTransaction(
        String nodeNameStr,
        String rscNameStr,
        boolean keepTiebreakerRef,
        ResponseContext context
    )
    {
        Resource rsc = ctrlApiDataLoader.loadRsc(nodeNameStr, rscNameStr, false);

        if (rsc == null)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.WARN_NOT_FOUND,
                    getRscDescription(nodeNameStr, rscNameStr) + " not found."
                )
            );
        }

        ctrlRscDeleteApiHelper.ensureNotInUse(rsc);
        ctrlRscDeleteApiHelper.ensureNotLastDisk(rsc);

        return activateIfLast(rsc).concatWith(
            scopeRunner.fluxInTransactionalScope(
                "Prepare resource delete",
                LockGuard.createDeferred(nodesMapLock.writeLock(), rscDfnMapLock.writeLock()),
                () -> prepareResourceDeleteInTransaction(nodeNameStr, rscNameStr, keepTiebreakerRef, context)
            )
        );
    }

    private Flux<ApiCallRc> prepareResourceDeleteInTransaction(
        String nodeNameStr,
        String rscNameStr,
        boolean keepTiebreakerRef,
        ResponseContext context
    )
    {
        Resource rsc = ctrlApiDataLoader.loadRsc(nodeNameStr, rscNameStr, false);

        if (rsc == null)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.WARN_NOT_FOUND,
                    getRscDescription(nodeNameStr, rscNameStr) + " not found."
                )
            );
        }

        ctrlRscDeleteApiHelper.ensureNotInUse(rsc);
        ctrlRscDeleteApiHelper.ensureNotLastDisk(rsc);

        AccessContext accCtx = peerAccCtx.get();

        Flux<ApiCallRc> flux;
        try
        {
            ResourceDefinition rscDfn = rsc.getResourceDefinition();
            Set<SnapshotDefinition> snapDfnsToUpdate = CtrlRscDeleteApiCallHandler.handleZfsRenameIfNeeded(apiCtx, rsc);

            if (LayerUtils.hasLayer(rsc.getLayerData(accCtx), DeviceLayerKind.DRBD))
            {
                ctrlRscDeleteApiHelper.markDrbdDeletedWithVolumes(rsc);

                ApiCallRcImpl responses = new ApiCallRcImpl();
                AutoHelperResult autoResult = autoHelper.manage(
                    new AutoHelperContext(
                        responses,
                        context,
                        rsc.getResourceDefinition()
                    ).withKeepTiebreaker(keepTiebreakerRef)
                );

                Flux<ApiCallRc> abortSnapShipFlux = snapShipAbortHandler
                    .abortSnapshotShippingPrivileged(rsc.getResourceDefinition());

                ctrlTransactionHelper.commit();

                flux = Flux.just(responses);
                Flux<ApiCallRc> next = Flux.empty();
                if (!autoResult.isPreventUpdateSatellitesForResourceDelete())
                {
                    // only mark resource as delete if automagic does not want to keep the resource
                    next = next.concatWith(deleteResourceOnPeers(nodeNameStr, rscNameStr, context));
                }
                next = next.concatWith(abortSnapShipFlux)
                    .concatWith(autoResult.getFlux());
                if (!autoResult.isPreventUpdateSatellitesForResourceDelete())
                {
                    String descriptionFirstLetterCaps = firstLetterCaps(getRscDescription(rsc));
                    responses.addEntries(
                        ApiCallRcImpl.singletonApiCallRc(
                            ApiCallRcImpl
                                .entryBuilder(
                                    ApiConsts.DELETED,
                                    descriptionFirstLetterCaps + " preparing for deletion."
                                )
                                .setDetails(descriptionFirstLetterCaps + " UUID is: " + rsc.getUuid())
                                .build()
                        )
                    );

                    ResourceName rscName = rsc.getResourceDefinition().getName();
                    flux = flux.concatWith(
                        ctrlSatelliteUpdateCaller.updateSatellites(rsc, next).transform(
                            updateResponses -> CtrlResponseUtils.combineResponses(
                                errorReporter,
                                updateResponses,
                                rscName,
                                Collections.singleton(rsc.getNode().getName()),
                                "Preparing deletion of resource on {0}",
                                "Preparing deletion of resource on {0}"
                            )
                        )
                    );
                }
                flux = flux.concatWith(next);
            }
            else
            {
                // no DRBD, skip this step and continue with actual deletion. Since we already
                // are in a transaction, we can just return that
                flux = deleteResourceOnPeersInTransaction(nodeNameStr, rscNameStr, context);
            }

            flux = flux.concatWith(
                CtrlSnapshotApiCallHandler.updateSnapDfns(ctrlSatelliteUpdateCaller, errorReporter, snapDfnsToUpdate)
            );
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(exc, "Prepare resource delete", ApiConsts.FAIL_ACC_DENIED_RSC);
        }

        return flux;
    }

    public static Set<SnapshotDefinition> handleZfsRenameIfNeeded(AccessContext accCtxRef, Resource rscRef)
    {
        final Set<SnapshotDefinition> snapDfnsToUpdate = new HashSet<>();
        ResourceDefinition rscDfn = rscRef.getResourceDefinition();
        try
        {
            if (hasZfsVlm(accCtxRef, rscRef))
            {
                NodeName nodeName = rscRef.getNode().getName();
                final Set<Props> snapPropsToUpdate = new HashSet<>();
                for (SnapshotDefinition snapDfn : rscDfn.getSnapshotDfns(accCtxRef))
                {
                    @Nullable Snapshot snap = snapDfn.getSnapshot(accCtxRef, nodeName);
                    if (snap != null)
                    {
                        Props snapProps = snap.getSnapProps(accCtxRef);
                        @Nullable String snapValue = snapProps.getProp(PROP_KEY_ZFS_RENAME_SUFFIX);
                        if (snapValue == null)
                        {
                            snapDfnsToUpdate.add(snapDfn);
                            snapPropsToUpdate.add(snapProps);
                        }
                    }
                }

                Props rscProps = rscRef.getProps(accCtxRef);
                @Nullable String rscRenameSuffix = rscProps.getProp(PROP_KEY_ZFS_RENAME_SUFFIX);

                // this "is already set" check prevents that a resource's ZFS_RENAME property is updated during
                // deleting a resource in multiple steps. This MUST NOT happen.
                // (not exactly sure how this could happen, since we are about to delete the resource... no idea how
                // someone should manage to create a snapshot in between, but who knows what the future holds :) )
                String nextRenameSufixStr = rscRenameSuffix == null ? TimeUtils.getZfsRenameTime() : rscRenameSuffix;

                for (Props snapProps : snapPropsToUpdate)
                {
                    // if the snapDfn has not yet been renamed, it will be renamed now.
                    snapProps.setProp(PROP_KEY_ZFS_RENAME_SUFFIX, nextRenameSufixStr);
                }

                if (!nextRenameSufixStr.equals(rscRenameSuffix))
                {
                    // although this resource is going to be deleted soon, the controller still needs to tell the
                    // satellite's ZfsProvider the target-rename-suffix
                    rscRef.getProps(accCtxRef).setProp(PROP_KEY_ZFS_RENAME_SUFFIX, nextRenameSufixStr);
                }
            }
        }
        catch (InvalidKeyException | NumberFormatException | InvalidValueException | AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
        return snapDfnsToUpdate;
    }

    private static boolean hasZfsVlm(AccessContext accCtxRef, Resource rscRef) throws AccessDeniedException
    {
        boolean ret = false;
        Set<StorPool> storPools = LayerVlmUtils.getStorPools(rscRef, accCtxRef);
        for (StorPool sp : storPools)
        {
            DeviceProviderKind spKind = sp.getDeviceProviderKind();
            if (spKind.equals(DeviceProviderKind.ZFS) || spKind.equals(DeviceProviderKind.ZFS_THIN))
            {
                ret = true;
                break;
            }
        }
        return ret;
    }

    private Flux<ApiCallRc> deleteResourceOnPeers(String nodeNameStr, String rscNameStr, ResponseContext context)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Deleting resource",
                LockGuard.createDeferred(nodesMapLock.writeLock(), rscDfnMapLock.writeLock()),
                () -> deleteResourceOnPeersInTransaction(nodeNameStr, rscNameStr, context)
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> deleteResourceOnPeersInTransaction(
        String nodeNameStr,
        String rscNameStr,
        ResponseContext context
    )
    {
        Resource rsc = ctrlApiDataLoader.loadRsc(nodeNameStr, rscNameStr, false);

        ApiCallRcImpl responses = new ApiCallRcImpl();
        Flux<ApiCallRc> flux;
        if (rsc == null)
        {
            responses.addEntries(
                ApiCallRcImpl.singleApiCallRc(
                    ApiConsts.WARN_NOT_FOUND | ApiConsts.MASK_RSC,
                    "Resource that should be deleted was no longer found"
                )
            );
            flux = Flux.just(responses);
            // no updateSatellites are required, since we are called within a flux-chain that will
            // call updateSatellites if necessary
        }
        else
        {
            Set<NodeName> nodeNamesToDelete = new TreeSet<>();
            NodeName nodeName = rsc.getNode().getName();

            ctrlRscDeleteApiHelper.markDeletedWithVolumes(rsc);
            nodeNamesToDelete.add(nodeName);

            AutoHelperResult autoResult = autoHelper.manage(
                new AutoHelperContext(
                    responses,
                    context,
                    rsc.getResourceDefinition()
                )
            );

            Flux<ApiCallRc> abortSnapShipFlux = snapShipAbortHandler.abortSnapshotShippingPrivileged(
                rsc.getResourceDefinition()
            );

            ctrlTransactionHelper.commit();

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

                ResourceName rscName = rsc.getResourceDefinition().getName();
                flux = flux.concatWith(
                    ctrlRscDeleteApiHelper.updateSatellitesForResourceDelete(context, nodeNamesToDelete, rscName)
                );
            }
            else
            {
                flux = Flux.just(responses);
            }
            flux = flux
                .concatWith(abortSnapShipFlux)
                .concatWith(autoResult.getFlux())
                .concatWith(ctrlRscDfnApiCallHandler.get().updateProps(rsc.getResourceDefinition()));
        }
        return flux;
    }

    private Flux<ApiCallRc> activateIfLast(Resource rsc)
    {
        Flux<ApiCallRc> ret = Flux.empty();

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
                ret = ctrlRscActivateApiCallHandler.activateRsc(
                    rsc.getNode().getName().displayValue,
                    rsc.getResourceDefinition().getName().displayValue
                );
            }
        }
        return ret;
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

    private void failIfDependentSnapshot(Resource rsc)
    {
        try
        {
            for (SnapshotDefinition snapshotDfn : rsc.getResourceDefinition().getSnapshotDfns(peerAccCtx.get()))
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
                                        "Resource '" + rsc.getResourceDefinition().getName() +
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
