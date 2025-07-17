package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.SharedResourceManager;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.controller.utils.ResourceDataUtils;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.resource.CtrlRscLayerDataFactory;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.satellitestate.SatelliteResourceState;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.utils.layer.LayerRscUtils;
import com.linbit.linstor.utils.layer.LayerVlmUtils;
import com.linbit.locks.LockGuard;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscApiCallHandler.getRscDescription;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscApiCallHandler.makeRscContext;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import reactor.core.publisher.Flux;

@Singleton
public class CtrlRscActivateApiCallHandler
{
    private final ErrorReporter errorReporter;
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final ResponseConverter responseConverter;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final Provider<AccessContext> peerAccCtx;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final SharedResourceManager sharedRscMgr;
    private final CtrlRscLayerDataFactory ctrlRscLayerDataFactory;

    @Inject
    public CtrlRscActivateApiCallHandler(
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        ResponseConverter responseConverterRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        SharedResourceManager sharedRscMgrRef,
        CtrlRscLayerDataFactory ctrlRscLayerDataFactoryRef,
        ErrorReporter errorReporterRef
    )
    {
        sharedRscMgr = sharedRscMgrRef;
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        responseConverter = responseConverterRef;
        peerAccCtx = peerAccCtxRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        ctrlRscLayerDataFactory = ctrlRscLayerDataFactoryRef;
        errorReporter = errorReporterRef;
    }

    public Flux<ApiCallRc> activateRsc(String nodeNameRef, String rscNameRef)
    {
        return setResourceActiveState(nodeNameRef, rscNameRef, true);
    }

    public Flux<ApiCallRc> deactivateRsc(String nodeNameRef, String rscNameRef)
    {
        return setResourceActiveState(nodeNameRef, rscNameRef, false);
    }

    private Flux<ApiCallRc> setResourceActiveState(String nodeNameStr, String rscNameStr, boolean active)
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
                () -> setResourceActiveStateInTransaction(
                    nodeNameStr,
                    rscNameStr,
                    active,
                    context
                )
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> setResourceActiveStateInTransaction(
        String nodeNameStrRef,
        String rscNameStrRef,
        boolean activate,
        ResponseContext contextRef
    )
    {
        Resource rsc = ctrlApiDataLoader.loadRsc(nodeNameStrRef, rscNameStrRef, true);
        Flux<ApiCallRc> ret;
        boolean isActive = !isFlagSet(rsc, Resource.Flags.INACTIVE);
        if (isActive == activate)
        {
            ret = Flux.just(
                new ApiCallRcImpl(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.INFO_NOOP,
                        String.format("Resource is already %s. Noop", activate ? "activated" : "deactivated")
                    )
                )
            );
        }
        else
        {
            if (activate)
            {
                checkIfReactivatable(rsc);

                unsetFlag(rsc, Resource.Flags.INACTIVE, Resource.Flags.INACTIVATING);
                setFlag(rsc, Resource.Flags.REACTIVATE);

                ResourceDataUtils.recalculateVolatileRscData(ctrlRscLayerDataFactory, rsc);

                ctrlTransactionHelper.commit();
                ret = ctrlSatelliteUpdateCaller.updateSatellites(rsc, Flux.empty()).transform(
                    updateResponses -> CtrlResponseUtils.combineResponses(
                        errorReporter,
                        updateResponses,
                        rsc.getResourceDefinition().getName(),
                        Collections.singleton(rsc.getNode().getName()),
                        "Reactivating resource on {0}",
                        "Resource updated on {0}"
                    ).concatWith(
                        completeActivation(
                            rsc.getNode().getName().displayValue,
                            rsc.getResourceDefinition().getName().displayValue
                        )
                    )
                );
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
                setFlag(rsc, Resource.Flags.INACTIVE, Resource.Flags.INACTIVATING);
                ResourceDataUtils.recalculateVolatileRscData(ctrlRscLayerDataFactory, rsc);

                ctrlTransactionHelper.commit();

                Flux<ApiCallRc> nextStep = finishInactivate(contextRef, nodeNameStrRef, rscNameStrRef);

                ret = ctrlSatelliteUpdateCaller.updateSatellites(rsc, nextStep).transform(
                    updateResponses -> CtrlResponseUtils.combineResponses(
                        errorReporter,
                        updateResponses,
                        rsc.getResourceDefinition().getName(),
                        Collections.singleton(rsc.getNode().getName()),
                        "Resource deactivated on {0}",
                        "Resource marked inactivate on {0}"
                    )
                )
                    .concatWith(nextStep);
            }
        }
        return ret;
    }

    private Flux<ApiCallRc> finishInactivate(ResponseContext context, String nodeNameStr, String rscNameStr)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Finishing deactivation of resource " +
                    CtrlRscApiCallHandler.getRscDescription(nodeNameStr, rscNameStr),
                createLockGuard(),
                () -> finishInactivateInTransaction(
                    nodeNameStr,
                    rscNameStr
                )
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> finishInactivateInTransaction(String nodeNameStrRef, String rscNameStrRef)
    {
        Resource rsc = ctrlApiDataLoader.loadRsc(nodeNameStrRef, rscNameStrRef, true);
        unsetFlag(rsc, Resource.Flags.INACTIVATING);
        ResourceDataUtils.recalculateVolatileRscData(ctrlRscLayerDataFactory, rsc);
        ctrlTransactionHelper.commit();

        return ctrlSatelliteUpdateCaller.updateSatellites(rsc, Flux.empty()).transform(
            updateResponses -> CtrlResponseUtils.combineResponses(
                errorReporter,
                updateResponses,
                rsc.getResourceDefinition().getName(),
                Collections.singleton(rsc.getNode().getName()),
                "Finished deactivation of resource on {0}",
                "Finished deactivation of resource on {0}"
            )
        );
    }

    private void checkIfReactivatable(Resource rsc)
    {
        if (hasDrbdInStack(rsc))
        {
            if (isFlagSet(rsc, Resource.Flags.INACTIVE_PERMANENTLY))
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_INVLD_LAYER_STACK,
                        "This DRDB-resource cannot be re-activated!"
                    )
                );
            }
            if (!sharedRscMgr.isActivationAllowed(rsc))
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_ONLY_ONE_ACT_RSC_PER_SHARED_STOR_POOL_ALLOWED,
                        "Only one active resource per shared storage pool allowed"
                    )
                );
            }
        }
    }

    private Flux<ApiCallRc> completeActivation(String nodeName, String rscName)
    {
        ResponseContext context = makeRscContext(
            ApiOperation.makeModifyOperation(),
            nodeName,
            rscName
        );

        return scopeRunner
            .fluxInTransactionalScope(
                "Completing activation of resource",
                createLockGuard(),
                () -> completeActivationInTransaction(
                    nodeName,
                    rscName,
                    context
                )
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> completeActivationInTransaction(
        String nodeNameRef,
        String rscNameRef,
        ResponseContext contextRef
    )
    {
        Resource rsc = ctrlApiDataLoader.loadRsc(nodeNameRef, rscNameRef, true);
        unsetFlag(rsc, Resource.Flags.REACTIVATE);
        ctrlTransactionHelper.commit();

        return ctrlSatelliteUpdateCaller.updateSatellites(rsc, Flux.empty()).transform(
            updateResponses -> CtrlResponseUtils.combineResponses(
                errorReporter,
                updateResponses,
                rsc.getResourceDefinition().getName(),
                Collections.singleton(rsc.getNode().getName()),
                "Resource activated on {0}",
                "Resource activated on {0}"
            )
        );
    }

    private boolean isFlagSet(Resource rsc, Resource.Flags... flags)
    {
        boolean ret;
        try
        {
            ret = rsc.getStateFlags().isSet(peerAccCtx.get(), flags);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access flags of " + getRscDescription(rsc),
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

    private boolean isResourceInUse(Resource rscRef)
    {
        boolean ret = false;
        try
        {
            // check if rscRef is an nvme- or ebs-target. if so, check if there is already an initiator -> inUse = true
            AccessContext peerCtx = peerAccCtx.get();
            boolean isNvmeTarget = false;
            boolean isEbsTarget = false;
            {
                List<DeviceLayerKind> layerStack = LayerRscUtils.getLayerStack(rscRef, peerCtx);
                isNvmeTarget = layerStack.contains(DeviceLayerKind.NVME);

                for (StorPool storPool : LayerVlmUtils.getStorPools(rscRef, peerCtx))
                {
                    if (storPool.getDeviceProviderKind().equals(DeviceProviderKind.EBS_TARGET))
                    {
                        isEbsTarget = true;
                    }
                }
            }
            if (isNvmeTarget || isEbsTarget)
            {
                Iterator<Resource> rscIt = rscRef.getResourceDefinition().iterateResource(peerCtx);
                while (rscIt.hasNext())
                {
                    Resource otherRsc = rscIt.next();
                    if (otherRsc.getStateFlags().isSomeSet(
                        peerCtx,
                        Resource.Flags.NVME_INITIATOR,
                        Resource.Flags.EBS_INITIATOR
                    ))
                    {
                        ret = true;
                        break;
                    }
                }
            }

            if (!ret)
            {
                // this is much more complicated than it should be ....
                SatelliteResourceState stltRscState = rscRef.getNode().getPeer(peerCtx)
                    .getSatelliteState()
                    .getResourceStates()
                    .get(rscRef.getResourceDefinition().getName());
                if (stltRscState != null && stltRscState.isInUse() != null)
                {
                    ret = stltRscState.isInUse();
                }
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "check if resource is in use. " + getRscDescription(rscRef),
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        return ret;
    }

    private boolean hasDrbdInStack(Resource rsc)
    {
        boolean hasDrbdInStack;
        try
        {
            hasDrbdInStack = LayerRscUtils.getLayerStack(rsc, peerAccCtx.get()).contains(DeviceLayerKind.DRBD);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "check if resource has DRBD in layer-list. " + getRscDescription(rsc),
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        return hasDrbdInStack;
    }

    private LockGuard createLockGuard()
    {
        return lockGuardFactory.buildDeferred(LockType.WRITE, LockObj.NODES_MAP, LockObj.RSC_DFN_MAP);
    }
}
