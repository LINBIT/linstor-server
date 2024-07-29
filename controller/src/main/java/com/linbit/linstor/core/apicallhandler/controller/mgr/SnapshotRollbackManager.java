package com.linbit.linstor.core.apicallhandler.controller.mgr;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlTransactionHelper;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscObject;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscObject.DrbdRscFlags;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;
import com.linbit.utils.StringUtils;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.slf4j.MDC;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

@Singleton
public class SnapshotRollbackManager
{
    private final AccessContext apiCtx;
    private final ErrorReporter errorReporter;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final ScopeRunner scopeRunner;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;

    private final HashMap<ResourceName, SnapRollbackInfo> infoMap = new HashMap<>();

    @Inject
    public SnapshotRollbackManager(
        @ApiContext AccessContext apiCtxRef,
        ErrorReporter errorReporterRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        ScopeRunner scopeRunnerRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef
    )
    {
        apiCtx = apiCtxRef;
        errorReporter = errorReporterRef;
        lockGuardFactory = lockGuardFactoryRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        scopeRunner = scopeRunnerRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
    }

    public Flux<ApiCallRc> prepareFlux(ResourceDefinition rscDfnRef, Set<NodeName> diskNodeNamesRef)
    {
        Flux<ApiCallRc> ret;
        synchronized (infoMap)
        {
            ResourceName rscName = rscDfnRef.getName();
            SnapRollbackInfo snapRollbackInfo = infoMap.get(rscName);
            if (snapRollbackInfo != null)
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_SNAPSHOT_ROLLBACK_IN_PROGRESS,
                        "A rollback for this resource is already in progress!"
                    )
                );
            }

            SnapRollbackInfo info = new SnapRollbackInfo(diskNodeNamesRef, rscDfnRef);
            ret = Flux.<ApiCallRc>create(sink ->
            {
                synchronized (info)
                {
                    info.fluxSink = sink;
                    info.notifyAll();
                }
            })
                .concatWith(recoverIfNeeded(info));
            infoMap.put(rscName, info);
        }
        return ret;
    }

    private Flux<ApiCallRc> recoverIfNeeded(SnapRollbackInfo infoRef)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Recovering rollback if needed",
                lockGuardFactory.buildDeferred(LockType.READ, LockObj.NODES_MAP, LockObj.RSC_DFN_MAP),
                () -> recoverIfNeedednInTransaction(infoRef)
            );
    }

    private Flux<ApiCallRc> recoverIfNeedednInTransaction(SnapRollbackInfo infoRef)
    {
        Flux<ApiCallRc> ret;
        if (!infoRef.failedNodes.isEmpty())
        {
            try
            {
                ResourceDefinition rscDfn = infoRef.rscDfn;
                ApiCallRcImpl responses = new ApiCallRcImpl();
                Flux<ApiCallRc> nextStep;

                // remove the rollback property
                Iterator<Resource> rscIter;
                rscIter = rscDfn.iterateResource(apiCtx);

                while (rscIter.hasNext())
                {
                    Resource rsc = rscIter.next();
                    rsc.getProps(apiCtx).removeProp(ApiConsts.KEY_RSC_ROLLBACK_TARGET);
                }

                if (infoRef.succeededNodes.isEmpty())
                {
                    // no resource succeeded. since we just removed the rollback property, there is nothing else we can
                    // or need to do here
                    nextStep = Flux.empty();
                    responses.add(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.INFO_ABORTED_FAILED_SNAPSHOT_ROLLBACK,
                            "All satellites failed the snapshot rollback. Aborting. Data remains unchanged."
                        )
                    );
                }
                else
                {
                    // some resources succeeded, some failed. tell the failed resources to delete recreate DRBD metadata
                    // if DRBD is involved. If DRBD is not involved, there is nothing we can do.
                    for (NodeName nodeName : infoRef.failedNodes)
                    {
                        Resource rsc = rscDfn.getResource(apiCtx, nodeName);
                        AbsRscLayerObject<Resource> layerData = rsc.getLayerData(apiCtx);
                        if (layerData instanceof DrbdRscData)
                        {
                            DrbdRscData<Resource> drbdRscData = (DrbdRscData<Resource>) layerData;
                            StateFlags<DrbdRscFlags> drbdRscFlags = drbdRscData.getFlags();
                            drbdRscFlags.disableFlags(apiCtx, DrbdRscObject.DrbdRscFlags.INITIALIZED);
                            drbdRscFlags.enableFlags(apiCtx, DrbdRscObject.DrbdRscFlags.FORCE_NEW_METADATA);
                        }
                    }
                    nextStep = disableForceNewMetadata(infoRef);
                    responses.add(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.INFO_RECOVERING_FAILED_SNAPSHOT_ROLLBACK,
                            "Satellites " + StringUtils.join(infoRef.failedNodes, ", ") +
                                " failed the snapshot rollback. Recreating resource on those nodes so they can sync up from nodes " +
                                StringUtils.join(infoRef.succeededNodes, ", ")

                        )
                    );
                }

                ctrlTransactionHelper.commit();

                ret = Flux.<ApiCallRc>just(responses)
                    .concatWith(
                        ctrlSatelliteUpdateCaller.updateSatellites(rscDfn, nextStep)
                            .transform(
                                updateResponses -> CtrlResponseUtils.combineResponses(
                                    errorReporter,
                                    updateResponses,
                                    rscDfn.getName(),
                                    infoRef.failedNodes,
                                    "Recovering resource {1} on {0} from rollback",
                                    "Finilized rollback {1} on {0}"
                                )
                            )
                        )
                    .concatWith(nextStep);
            }
            catch (AccessDeniedException | InvalidKeyException exc)
            {
                throw new ImplementationError(exc);
            }
            catch (DatabaseException exc)
            {
                throw new ApiDatabaseException(exc);
            }
        }
        else
        {
            // nothing failed, all good.
            ret = Flux.empty();
        }
        return ret;
    }

    private Flux<ApiCallRc> disableForceNewMetadata(SnapRollbackInfo infoRef)
    {
        return scopeRunner
            .fluxInTransactionlessScope(
                "Finishing recovery",
                lockGuardFactory.buildDeferred(LockType.READ, LockObj.NODES_MAP, LockObj.RSC_DFN_MAP),
                () -> disableForceNewMetadataInTransaction(infoRef),
                MDC.getCopyOfContextMap()
            );
    }

    private Flux<ApiCallRc> disableForceNewMetadataInTransaction(SnapRollbackInfo infoRef)
    {
        ResourceDefinition rscDfn = infoRef.rscDfn;
        try
        {
            for (NodeName nodeName : infoRef.failedNodes)
            {
                Resource rsc = rscDfn.getResource(apiCtx, nodeName);
                AbsRscLayerObject<Resource> layerData = rsc.getLayerData(apiCtx);
                if (layerData instanceof DrbdRscData)
                {
                    DrbdRscData<Resource> drbdRscData = (DrbdRscData<Resource>) layerData;
                    StateFlags<DrbdRscFlags> drbdRscFlags = drbdRscData.getFlags();
                    drbdRscFlags.disableFlags(apiCtx, DrbdRscObject.DrbdRscFlags.INITIALIZED);
                    drbdRscFlags.enableFlags(apiCtx, DrbdRscObject.DrbdRscFlags.FORCE_NEW_METADATA);
                }
            }
            ctrlTransactionHelper.commit();
        }
        catch (AccessDeniedException | InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }

        return ctrlSatelliteUpdateCaller.updateSatellites(rscDfn, Flux.<ApiCallRc>empty())
            .transform(
                updateResponses -> CtrlResponseUtils.combineResponses(
                    errorReporter,
                    updateResponses,
                    rscDfn.getName(),
                    "Finishing recovery for resource {1} on {0} from rollback"
                )
            );
    }

    public void handle(NodeName nodeNameRef, ResourceName rscNameRef, boolean successRef)
    {
        @Nullable SnapRollbackInfo snapRollbackInfo;
        synchronized (infoMap)
        {
            snapRollbackInfo = infoMap.get(rscNameRef);
        }
        if (snapRollbackInfo != null)
        {
            boolean allFinished = snapRollbackInfo.handle(nodeNameRef, successRef);

            if (allFinished)
            {
                synchronized (snapRollbackInfo)
                {
                    while (snapRollbackInfo.fluxSink == null)
                    {
                        try
                        {
                            snapRollbackInfo.wait();
                        }
                        catch (InterruptedException exc)
                        {
                            errorReporter.reportError(exc);
                        }
                    }
                }
                synchronized (infoMap)
                {
                    infoMap.remove(rscNameRef);
                }
                snapRollbackInfo.fluxSink.complete();
            }
        }
    }

    private static class SnapRollbackInfo
    {
        private final Set<NodeName> waitingForNodes;
        private final Set<NodeName> succeededNodes = new HashSet<>();
        private final Set<NodeName> failedNodes = new HashSet<>();
        private final ResourceDefinition rscDfn;

        private @Nullable FluxSink<ApiCallRc> fluxSink;

        SnapRollbackInfo(Set<NodeName> waitingForNodesRef, ResourceDefinition rscDfnRef)
        {
            rscDfn = rscDfnRef;
            waitingForNodes = new HashSet<>(waitingForNodesRef);
        }

        /**
         * Adds the nodeName to the succeededNodes set if successRef is true, otherwise to the set of failedNodes.
         * If a nodeName was once in the failedNodes but succeeds now, the entry will be removed from the failedNodes.
         * If a nodeName is already in the succeededNodes but fails now, the entry will remain in the succeededNodes,
         * and will NOT be added to the failedNodes.
         *
         * In any cases, the entry will be deleted from the waitingForNodes set.
         *
         * @param nodeNameRef
         * @param successRef
         *
         * @return True if after calling this method the waitingForNodes set is empty
         */
        public boolean handle(NodeName nodeNameRef, boolean successRef)
        {
            if (successRef)
            {
                succeededNodes.add(nodeNameRef);
                // if the nodeName was also in the failedNodes, it was most likely an implementation bug, but for now
                // its good enough for us to consider it as succeeded
                failedNodes.remove(nodeNameRef);
            }
            else
            {
                if (!succeededNodes.contains(nodeNameRef))
                {
                    failedNodes.add(nodeNameRef);
                } // else, it is most likely an implementation bug, but we consider this still as successful
            }
            waitingForNodes.remove(nodeNameRef);
            return waitingForNodes.isEmpty();
        }
    }
}
