package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.satellitestate.SatelliteResourceState;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.locks.LockGuard;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscApiCallHandler.getRscDescription;
import static com.linbit.utils.StringUtils.firstLetterCaps;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Collections;
import java.util.Iterator;
import java.util.UUID;

import reactor.core.publisher.Flux;

@Singleton
public class CtrlRscDeleteApiHelper
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final ScopeRunner scopeRunner;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final Provider<AccessContext> peerAccCtx;
    private LockGuardFactory lockGuardFactory;

    @Inject
    public CtrlRscDeleteApiHelper(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        ScopeRunner scopeRunnerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        LockGuardFactory lockGuardFactoryRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        scopeRunner = scopeRunnerRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        lockGuardFactory = lockGuardFactoryRef;
        peerAccCtx = peerAccCtxRef;
    }

    public void markDeletedWithVolumes(Resource rsc)
    {
        try
        {
            rsc.markDeleted(peerAccCtx.get());
            Iterator<Volume> volumesIterator = rsc.iterateVolumes();
            while (volumesIterator.hasNext())
            {
                Volume vlm = volumesIterator.next();
                vlm.markDeleted(peerAccCtx.get());
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "mark " + getRscDescription(rsc) + " as deleted",
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
    }

    // Restart from here when connection established and DELETE flag set
    public Flux<ApiCallRc> updateSatellitesForResourceDelete(NodeName nodeName, ResourceName rscName)
    {
        return scopeRunner
            .fluxInTransactionlessScope(
                "Update for resource deletion",
                lockGuardFactory.buildDeferred(LockType.READ, LockObj.RSC_DFN_MAP),
                () -> updateSatellitesInScope(nodeName, rscName)
            );
    }

    private Flux<ApiCallRc> updateSatellitesInScope(NodeName nodeName, ResourceName rscName)
    {
        Resource rsc = ctrlApiDataLoader.loadRsc(nodeName, rscName, false);

        Flux<ApiCallRc> flux;

        if (rsc == null)
        {
            flux = Flux.empty();
        }
        else
        {
            flux = ctrlSatelliteUpdateCaller.updateSatellites(rsc)
                .transform(updateResponses -> CtrlResponseUtils.combineResponses(
                    updateResponses,
                    rscName,
                    Collections.singleton(nodeName),
                    "Deleted {1} on {0}",
                    "Notified {0} that {1} is being deleted on ''" + nodeName + "''"
                ))
                .concatWith(deleteData(nodeName, rscName))
                .onErrorResume(CtrlResponseUtils.DelayedApiRcException.class, ignored -> Flux.empty());
        }

        return flux;
    }

    public Flux<ApiCallRc> deleteData(NodeName nodeName, ResourceName rscName)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Delete resource data",
                lockGuardFactory.buildDeferred(LockType.WRITE, LockObj.RSC_DFN_MAP),
                () -> deleteDataInTransaction(nodeName, rscName)
            );
    }

    private Flux<ApiCallRc> deleteDataInTransaction(NodeName nodeName, ResourceName rscName)
    {
        Resource rsc = ctrlApiDataLoader.loadRsc(nodeName, rscName, false);

        Flux<ApiCallRc> flux;

        if (rsc == null)
        {
            flux = Flux.empty();
        }
        else
        {
            UUID rscUuid = rsc.getUuid();
            String descriptionFirstLetterCaps = firstLetterCaps(getRscDescription(rsc));
            ResourceDefinition rscDfn = rsc.getDefinition();

            deletePrivileged(rsc);

            if (rscDfn.getResourceCount() == 0)
            {
                // remove primary flag
                errorReporter.logDebug(
                    String.format("Resource definition '%s' empty, deleting primary flag.", rscName)
                );
                removePropPrimarySetPrivileged(rscDfn);
            }

            ctrlTransactionHelper.commit();

            flux = Flux.just(ApiCallRcImpl.singletonApiCallRc(ApiCallRcImpl
                .entryBuilder(ApiConsts.DELETED, descriptionFirstLetterCaps + " deletion complete.")
                .setDetails(descriptionFirstLetterCaps + " UUID was: " + rscUuid)
                .build()
            ));
        }

        return flux;
    }

    public void ensureNotInUse(Resource rsc)
    {
        ResourceName rscName = rsc.getDefinition().getName();
        NodeName nodeName = rsc.getAssignedNode().getName();

        Boolean inUse;
        Peer peer = getPeerPrivileged(rsc.getAssignedNode());
        try (LockGuard ignored = LockGuard.createLocked(peer.getSatelliteStateLock().readLock()))
        {
            inUse = peer.getSatelliteState().getFromResource(
                rscName, SatelliteResourceState::isInUse);
        }

        if (inUse != null && inUse)
        {
            throw new ApiRcException(ApiCallRcImpl
                .entryBuilder(
                    ApiConsts.FAIL_IN_USE,
                    String.format("Resource '%s' is still in use.", rscName)
                )
                .setCause("Resource is mounted/in use.")
                .setCorrection(String.format("Un-mount resource '%s' on the node '%s'.", rscName, nodeName))
                .build()
            );
        }
    }

    private Peer getPeerPrivileged(Node node)
    {
        Peer nodePeer;
        try
        {
            nodePeer = node.getPeer(apiCtx);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return nodePeer;
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

    private void removePropPrimarySetPrivileged(ResourceDefinition rscDfn)
    {
        try
        {
            rscDfn.getProps(apiCtx).removeProp(InternalApiConsts.PROP_PRIMARY_SET);
        }
        catch (InvalidKeyException | AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
    }
}
