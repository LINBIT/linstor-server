package com.linbit.linstor.core.apicallhandler.controller;

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
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.locks.LockGuard;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscApiCallHandler.getRscDescription;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscApiCallHandler.makeRscContext;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscDfnApiCallHandler.getRscDfnDescription;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Collections;

import reactor.core.publisher.Flux;

@Singleton
public class CtrlRscActivateApiCallHandler
{
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final ResponseConverter responseConverter;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final Provider<AccessContext> peerAccCtx;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final CtrlTransactionHelper ctrlTransactionHelper;

    @Inject
    public CtrlRscActivateApiCallHandler(
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        ResponseConverter responseConverterRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef
    )
    {
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        responseConverter = responseConverterRef;
        peerAccCtx = peerAccCtxRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;

    }

    public Flux<ApiCallRc> activateRsc(String nodeNameRef, String rscNameRef)
    {
        return setResourceActive(nodeNameRef, rscNameRef, true);
    }

    public Flux<ApiCallRc> deactivateRsc(String nodeNameRef, String rscNameRef)
    {
        return setResourceActive(nodeNameRef, rscNameRef, false);
    }

    private Flux<ApiCallRc> setResourceActive(String nodeNameStr, String rscNameStr, boolean active)
    {
        ResponseContext context = makeRscContext(
            ApiOperation.makeModifyOperation(),
            nodeNameStr,
            rscNameStr
        );

        return scopeRunner
            .fluxInTransactionalScope(
                active ? "Activate resource" : "Deactivate resource",
                createLockGuard(),
                () -> setResourceActiveInTransaction(
                    nodeNameStr,
                    rscNameStr,
                    active,
                    context
                )
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> setResourceActiveInTransaction(
        String nodeNameStrRef,
        String rscNameStrRef,
        boolean active,
        ResponseContext contextRef
    )
    {
        Resource rsc = ctrlApiDataLoader.loadRsc(nodeNameStrRef, rscNameStrRef, true);
        Flux<ApiCallRc> ret;
        if (isRscActive(rsc) == active)
        {
            ret = Flux.just(
                new ApiCallRcImpl(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.INFO_NOOP,
                        String.format("Resource is already %s. Noop", active ? "activated" : "deactivated")
                    )
                )
            );
        }
        else
        {
            if (active)
            {
                if (isSnapshotShippingInProgress(rsc))
                {
                    throw new ApiRcException(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_SNAPSHOT_SHIPPING_IN_PROGRESS,
                            "Cannot activate a resource while being a snapshot-shipping-target!"
                        )
                    );
                }
                unsetFlag(rsc, Resource.Flags.INACTIVE);
            }
            else
            {
                if (isResourceInUse(rsc))
                {
                    throw new ApiRcException(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_IN_USE,
                            "Cannot deactivate a resource while being in use!"
                        )
                    );
                }
                setFlag(rsc, Resource.Flags.INACTIVE);
            }
            ctrlTransactionHelper.commit();

            ret = ctrlSatelliteUpdateCaller.updateSatellites(rsc, Flux.empty()).transform(
                updateResponses -> CtrlResponseUtils.combineResponses(
                    updateResponses,
                    rsc.getResourceDefinition().getName(),
                    Collections.singleton(rsc.getNode().getName()),
                    "Resource deactivated on {0}",
                    "Resource marked inactivate on {0}"
                )
            );
        }
        return ret;
    }

    private boolean isRscActive(Resource rscRef)
    {
        boolean ret;
        try
        {
            ret = !rscRef.getStateFlags().isSet(peerAccCtx.get(), Resource.Flags.INACTIVE);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access flags of " + getRscDescription(rscRef),
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        return ret;
    }

    private void unsetFlag(Resource rscRef, Flags... flags)
    {
        try
        {
            rscRef.getStateFlags().disableFlags(peerAccCtx.get(), flags);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "disable flags of " + getRscDescription(rscRef),
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
    }

    private void setFlag(Resource rscRef, Flags... flags)
    {
        try
        {
            rscRef.getStateFlags().enableFlags(peerAccCtx.get(), flags);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "enable flags of " + getRscDescription(rscRef),
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
    }

    private boolean isSnapshotShippingInProgress(Resource rscRef)
    {
        boolean ret = false;
        try
        {
            for (SnapshotDefinition snapDfn : rscRef.getResourceDefinition().getSnapshotDfns(peerAccCtx.get()))
            {
                if (snapDfn.getFlags().isSet(peerAccCtx.get(), SnapshotDefinition.Flags.SHIPPING))
                {
                    ret = true;
                    break;
                }
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access snapshot-definitions of " + getRscDfnDescription(rscRef.getResourceDefinition()),
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }
        return ret;
    }

    private boolean isResourceInUse(Resource rscRef)
    {
        boolean ret;
        try
        {
            // this is much more complicated than it should be ....
            ret = rscRef.getNode().getPeer(peerAccCtx.get()).getSatelliteState().getResourceStates().get(
                rscRef.getDefinition().getName()
            ).isInUse();
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "check if resource is in use. " + getRscDescription(rscRef),
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        return ret;
    }

    private LockGuard createLockGuard()
    {
        return lockGuardFactory.buildDeferred(LockType.WRITE, LockObj.NODES_MAP, LockObj.RSC_DFN_MAP);
    }
}
