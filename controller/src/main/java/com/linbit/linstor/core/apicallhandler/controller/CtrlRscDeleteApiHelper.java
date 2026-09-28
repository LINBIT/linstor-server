package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.autohelper.AutoHelperContext;
import com.linbit.linstor.core.apicallhandler.controller.autohelper.CtrlRscAutoHelper;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.satellitestate.SatelliteResourceState;
import com.linbit.linstor.tasks.RetryResourcesTask;
import com.linbit.linstor.tasks.ScheduleBackupService;
import com.linbit.locks.LockGuard;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;

import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.slf4j.MDC;
import reactor.core.publisher.Flux;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscApiCallHandler.getRscDescription;
import static com.linbit.utils.StringUtils.firstLetterCaps;

@Singleton
public class CtrlRscDeleteApiHelper
{
    public static final String FULL_KEY_ZFS_DELETE_STRATEGY = ApiConsts.NAMESPC_STORAGE_DRIVER +
        ReadOnlyProps.PATH_SEPARATOR + ApiConsts.NAMESPC_ZFS +
        ReadOnlyProps.PATH_SEPARATOR + ApiConsts.KEY_ZFS_DELETE_STRATEGY;

    private final ErrorReporter errorReporter;
    private final ScopeRunner scopeRunner;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final LockGuardFactory lockGuardFactory;
    private final ScheduleBackupService scheduleService;
    private final RetryResourcesTask retryRscTask;
    private final Provider<CtrlRscAutoHelper> rscAutoHelperProvider;
    private final CtrlMinIoSizeHelper minIoSizeHelper;

    @Inject
    public CtrlRscDeleteApiHelper(
        ErrorReporter errorReporterRef,
        ScopeRunner scopeRunnerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        LockGuardFactory lockGuardFactoryRef,
        ScheduleBackupService scheduleServiceRef,
        RetryResourcesTask retryRscTaskRef,
        Provider<CtrlRscAutoHelper> rscAutoHelperProviderRef,
        CtrlMinIoSizeHelper ctrlMinIoSizeHelperRef
    )
    {
        errorReporter = errorReporterRef;
        scopeRunner = scopeRunnerRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        lockGuardFactory = lockGuardFactoryRef;
        scheduleService = scheduleServiceRef;
        retryRscTask = retryRscTaskRef;
        rscAutoHelperProvider = rscAutoHelperProviderRef;
        minIoSizeHelper = ctrlMinIoSizeHelperRef;
    }

    public void markDeletedWithVolumes(Resource rsc)
    {
        try
        {
            ResourceDefinition rscDfn = rsc.getResourceDefinition();
            rsc.markDeleted();
            if (rscDfn.getNotDeletedDiskfulCount() == 0)
            {
                scheduleService.removeTasks(rscDfn);
            }

            Iterator<Volume> volumesIterator = rsc.iterateVolumes();
            while (volumesIterator.hasNext())
            {
                Volume vlm = volumesIterator.next();
                vlm.markDeleted();
            }
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
    }

    public void markDrbdDeletedWithVolumes(Resource rsc)
    {
        try
        {
            rsc.markDrbdDeleted();
            Iterator<Volume> volumesIterator = rsc.iterateVolumes();
            while (volumesIterator.hasNext())
            {
                Volume vlm = volumesIterator.next();
                vlm.markDrbdDeleted();
            }
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
    }

    // Restart from here when connection established and DELETE flag set
    public Flux<ApiCallRc> updateSatellitesForResourceDelete(
        ResponseContext contextRef,
        Set<NodeName> nodeNames,
        ResourceName rscName
    )
    {
        return scopeRunner
            .fluxInTransactionlessScope(
                "Update for resource deletion",
                lockGuardFactory.buildDeferred(LockType.READ, LockObj.RSC_DFN_MAP),
                () -> updateSatellitesInScope(contextRef, nodeNames, rscName),
                MDC.getCopyOfContextMap()
            );
    }

    private Flux<ApiCallRc> updateSatellitesInScope(
        ResponseContext contextRef,
        Set<NodeName> nodeNames,
        ResourceName rscName
    )
    {
        ResourceDefinition rscDfn = null;
        for (NodeName nodeName : nodeNames)
        {
            @Nullable Resource rsc = ctrlApiDataLoader.loadRscOrNull(nodeName, rscName);
            if (rsc != null && !rsc.isDeleted())
            {
                rscDfn = rsc.getResourceDefinition();
                break;
            }
        }

        NodeName[] nodeNamesArr = nodeNames.toArray(new NodeName[nodeNames.size()]);
        Flux<ApiCallRc> flux;
        if (rscDfn != null)
        {
            Flux<ApiCallRc> nextStep = deleteData(contextRef, nodeNames, rscName);
            flux = ctrlSatelliteUpdateCaller.updateSatellites(
                rscDfn,
                // will result in only a WARN ApiCallRc, but delivered as Flux.error. This prevents deleting database
                // entries when the node is offline / did not clean up properly. Otherwise we would leave orphaned
                // DRBD resources behind
                CtrlSatelliteUpdateCaller.notConnectedErrorForNodesWarnForOthers(nodeNamesArr),
                nextStep
            )
                .transform(updateResponses -> CtrlResponseUtils.combineResponses(
                    errorReporter,
                    updateResponses,
                    rscName,
                    nodeNames,
                    "Cleaning up {1} on {0}",
                    "Notified {0} that {1} is being cleaned up on Node(s): '" + nodeNames + "'"
                )
                )
                .concatWith(nextStep)
                .onErrorResume(CtrlResponseUtils.DelayedApiRcException.class, ignored -> Flux.empty());
        }
        else
        {
            flux = Flux.empty();
        }
        return flux;
    }

    public Flux<ApiCallRc> deleteData(ResponseContext contextRef, Set<NodeName> nodeNames, ResourceName rscName)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Delete resource data",
                lockGuardFactory.buildDeferred(LockType.WRITE, LockObj.RSC_DFN_MAP),
                () -> deleteDataInTransaction(contextRef, nodeNames, rscName)
            );
    }

    private Flux<ApiCallRc> deleteDataInTransaction(
        ResponseContext contextRef,
        Set<NodeName> nodeNames,
        ResourceName rscName
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        List<Resource> rscList = new ArrayList<>();
        for (NodeName nodeName : nodeNames)
        {
            @Nullable Resource rsc = ctrlApiDataLoader.loadRscOrNull(nodeName, rscName);
            if (rsc != null && !rsc.isDeleted())
            {
                // if we still run into "delete vs undelete" races, we should use UUIDs to find the resources we want
                // to delete. For now we stay with "only" flag-checking
                if (rsc.getStateFlags().isSet(Resource.Flags.DELETE))
                {
                    rscList.add(rsc);
                }
                else
                {
                    addDeleteCanceledEntry(apiCallRc, rsc);
                }
            }
        }

        Flux<ApiCallRc> flux;

        if (rscList.isEmpty())
        {
            flux = Flux.just(apiCallRc);
        }
        else
        {
            Set<ResourceDefinition> rscDfnsToCheck = new HashSet<>();
            for (Resource rsc : rscList)
            {
                UUID rscUuid = rsc.getUuid();
                String descriptionFirstLetterCaps = firstLetterCaps(getRscDescription(rsc));
                ResourceDefinition rscDfn = rsc.getResourceDefinition();

                cleanupAndDelete(rsc);

                rscDfnsToCheck.add(rscDfn);

                apiCallRc.addEntry(
                    ApiCallRcImpl
                        .entryBuilder(ApiConsts.DELETED, descriptionFirstLetterCaps + " deletion complete.")
                        .setDetails(descriptionFirstLetterCaps + " UUID was: " + rscUuid)
                        .build()
                );
            }

            Flux<ApiCallRc> autoFlux = Flux.empty();
            for (ResourceDefinition rscDfn : rscDfnsToCheck)
            {
                // check auto-helpers if we need a tiebreaker or something since we just deleted a resource
                autoFlux = autoFlux.concatWith(
                    rscAutoHelperProvider.get()
                        .manage(
                            new AutoHelperContext(apiCallRc, contextRef, rscDfn)
                        )
                        .flux()
                );

                // maybe we should create an auto-helper from this..
                if (rscDfn.getResourceCount() == 0)
                {
                    // remove primary flag
                    errorReporter.logDebug(
                        String.format("Resource definition '%s' empty, deleting primary flag.", rscName)
                    );
                    removePropPrimarySetPrivileged(rscDfn);

                    try
                    {
                        Iterator<VolumeDefinition> vlmDfnIt = rscDfn.iterateVolumeDfn();
                        while (vlmDfnIt.hasNext())
                        {
                            VolumeDefinition vlmDfn = vlmDfnIt.next();
                            if (minIoSizeHelper.isAutoMinIoSize(vlmDfn))
                            {
                                errorReporter.logDebug(
                                    "updateVolumeMinIoSize: Last resource deleted. Unsetting property " +
                                        "namespace = \"%s\", key = \"%s\" ",
                                    ApiConsts.NAMESPC_DRBD_DISK_OPTIONS,
                                    InternalApiConsts.KEY_DRBD_BLOCK_SIZE
                                );
                                Props vlmDfnProps = vlmDfn.getProps();
                                vlmDfnProps.removeProp(
                                    InternalApiConsts.KEY_DRBD_BLOCK_SIZE,
                                    ApiConsts.NAMESPC_DRBD_DISK_OPTIONS
                                );
                                vlmDfnProps.removeProp(
                                    ApiConsts.KEY_DRBD_FREEZE_BLOCK_SIZE,
                                    ApiConsts.NAMESPC_LINSTOR_DRBD
                                );
                            }
                        }
                    }
                    catch (InvalidKeyException exc)
                    {
                        throw new ImplementationError(exc);
                    }
                    catch (DatabaseException exc)
                    {
                        throw new ApiDatabaseException(exc);
                    }
                }
            }

            ctrlTransactionHelper.commit();

            errorReporter.logInfo("Resource deleted %s/%s", nodeNames, rscName);

            flux = Flux.<ApiCallRc>just(apiCallRc)
                .concatWith(autoFlux);
        }

        return flux;
    }

    static void addDeleteCanceledEntry(ApiCallRcImpl apiCallRc, Resource rsc)
    {
        apiCallRc.addEntries(
            ApiCallRcImpl.singleApiCallRc(
                // no explicit ApiConst for this quite rare condition
                ApiConsts.MASK_RSC | ApiConsts.MASK_DEL | ApiConsts.MASK_WARN,
                String.format(
                    "Deletion of %s was canceled by a concurrent operation",
                    getRscDescription(rsc)
                )
            )
        );
    }

    public ApiCallRc ensureNotInUse(Resource rsc)
    {
        return ensureNotInUse(rsc, true);
    }

    public ApiCallRc ensureNotInUse(Resource rsc, boolean throwApiExc)
    {
        ApiCallRcImpl resp = new ApiCallRcImpl();
        ResourceName rscName = rsc.getResourceDefinition().getName();
        NodeName nodeName = rsc.getNode().getName();

        Boolean inUse;
        Peer peer = getPeerPrivileged(rsc.getNode());
        try (LockGuard ignored = LockGuard.createLocked(peer.getSatelliteStateLock().readLock()))
        {
            inUse = peer.getSatelliteState().getFromResource(
                rscName, SatelliteResourceState::isInUseOrOpen);
        }

        if (inUse != null && inUse)
        {
            ApiCallRcImpl.ApiCallRcEntry err = CtrlRscInUseHelper.addInUseDetails(
                ApiCallRcImpl.entryBuilder(
                    ApiConsts.FAIL_IN_USE,
                    String.format("Resource '%s' is still in use.", rscName)
                ),
                nodeName
            ).build();
            resp.addEntry(err);
            if (throwApiExc)
            {
                throw new ApiRcException(err);
            }
        }

        return resp;
    }

    public ApiCallRc ensureNotLastDisk(Resource rsc)
    {
        return ensureNotLastDisk(rsc, true);
    }

    public ApiCallRc ensureNotLastDisk(Resource rsc, boolean throwApiExc)
    {
        ApiCallRcImpl resp = new ApiCallRcImpl();
        boolean isDiskless = rsc.isDrbdDiskless() ||
            rsc.isNvmeInitiator() ||
            rsc.isEbsInitiator();
        boolean hasDisklessNotDeleting = rsc.getResourceDefinition().hasDisklessNotDeleting();
        int otherNotDeletedDiskfulCount = rsc.getResourceDefinition()
            .getNotDeletedDiskfulCountExcluding(rsc);
        if (!isDiskless && hasDisklessNotDeleting && otherNotDeletedDiskfulCount == 0)
        {
            ApiCallRcImpl.ApiCallRcEntry err = ApiCallRcImpl
                .entryBuilder(
                    ApiConsts.FAIL_IN_USE,
                    String.format(
                        "Last resource of '%s' with disk still has diskless resources attached.",
                        rsc.getResourceDefinition().getName())
                )
                .setCause("Resource still has diskless users.")
                .setCorrection("Before deleting this resource, delete the diskless resources attached to it.")
                .setSkipErrorReport(true)
                .build();

            resp.addEntry(err);
            if (throwApiExc)
            {
                throw new ApiRcException(err);
            }
        }
        return resp;
    }

    private Peer getPeerPrivileged(Node node)
    {
        Peer nodePeer;
        nodePeer = node.getPeer();
        return nodePeer;
    }

    public void cleanupAndDelete(Resource rsc)
    {
        retryRscTask.remove(rsc);
        deletePrivileged(rsc);
    }

    private void deletePrivileged(Resource rsc)
    {
        try
        {
            rsc.delete();
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
    }

    private void removePropPrimarySetPrivileged(ResourceDefinition rscDfn)
    {
        try
        {
            rscDfn.getProps().removeProp(InternalApiConsts.DEPRECATED_PROP_PRIMARY_SET);
            Iterator<VolumeDefinition> vlmDfnIt = rscDfn.iterateVolumeDfn();
            while (vlmDfnIt.hasNext())
            {
                vlmDfnIt.next().uninitializeDrbd();
            }
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
    }
}
