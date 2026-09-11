package com.linbit.linstor.core.apicallhandler.controller.internal;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiDataLoader;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscAutoBalanceHelper;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscCrtApiHelper;
import com.linbit.linstor.core.apicallhandler.controller.CtrlTransactionHelper;
import com.linbit.linstor.core.apicallhandler.controller.CtrlVlmDfnModifyApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.utils.ResourceDataUtils;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.resource.CtrlRscLayerDataFactory;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.locks.LockGuardFactory;

import static com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller.notConnectedError;

import jakarta.inject.Inject;
import jakarta.inject.Provider;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;

import reactor.core.publisher.Flux;

public class RscDfnInternalCallHandler
{
    private final ErrorReporter errorReporter;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final CtrlRscCrtApiHelper ctrlRscCrtHelper;
    private final CtrlRscAutoBalanceHelper ctrlRscAutoBalanceHelper;
    private final Provider<Peer> peer;
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final CtrlVlmDfnModifyApiCallHandler ctrlVlmDfnModifyApiCallHandler;

    private final CtrlRscLayerDataFactory ctrlRscLayerDataFactory;

    @Inject
    public RscDfnInternalCallHandler(
        ErrorReporter errorReporterRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        CtrlRscCrtApiHelper crtlRscCrtHelperRef,
        CtrlRscAutoBalanceHelper ctrlRscAutoBalanceHelperRef,
        Provider<Peer> peerRef,
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        CtrlVlmDfnModifyApiCallHandler ctrlVlmDfnModifyApiCallHandlerRef,
        CtrlRscLayerDataFactory ctrlRscLayerDataFactoryRef
    )
    {
        errorReporter = errorReporterRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        ctrlRscCrtHelper = crtlRscCrtHelperRef;
        ctrlRscAutoBalanceHelper = ctrlRscAutoBalanceHelperRef;
        peer = peerRef;
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        ctrlVlmDfnModifyApiCallHandler = ctrlVlmDfnModifyApiCallHandlerRef;
        ctrlRscLayerDataFactory = ctrlRscLayerDataFactoryRef;
    }

    private Flux<ApiCallRc> markRscDfnFailed(String rscName)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Mark resource definition failed",
                lockGuardFactory.create()
                    .write(LockGuardFactory.LockObj.RSC_DFN_MAP)
                    .buildDeferred(),
                () -> markRscDfnFailedInTransaction(rscName)
            );
    }

    private Flux<ApiCallRc> markRscDfnFailedInTransaction(String rscName)
    {
        ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(rscName);
        try
        {
            rscDfn.getFlags().enableFlags(ResourceDefinition.Flags.FAILED);
        }
        catch (DatabaseException dbExc)
        {
            throw new ApiDatabaseException(dbExc);
        }
        ctrlTransactionHelper.commit();

        return ctrlSatelliteUpdateCaller
            .updateSatellites(rscDfn, notConnectedError(), Flux.empty())
            .transform(
                responses -> CtrlResponseUtils.combineResponses(
                    errorReporter,
                    responses,
                    rscDfn.getName(),
                    "Resource definition {1} marked failed."
                )
            );
    }

    public Flux<ApiCallRc> handleCloneUpdate(String rscName, int vlmNr, boolean success) {
        return scopeRunner
            .fluxInTransactionalScope(
                "Finish clone update",
                lockGuardFactory.create()
                    .write(LockGuardFactory.LockObj.RSC_DFN_MAP, LockGuardFactory.LockObj.NODES_MAP).buildDeferred(),
                () -> handleCloneUpdateInTransaction(rscName, vlmNr, success)
            )
            .onErrorResume(exc -> {
                errorReporter.reportError(exc);
                return markRscDfnFailed(rscName);
            });
    }

    public Flux<ApiCallRc> handleCloneUpdateInTransaction(String rscName, int vlmNr, boolean success) {
        Flux<ApiCallRc> flux = Flux.empty();
        Peer currentPeer = peer.get();

        ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(rscName);

        try
        {
            {
                Resource rsc = rscDfn.getResource(currentPeer.getNode().getName());
                Volume vlm = rsc.getVolume(new VolumeNumber(vlmNr));
                if (vlm.getFlags().isSet(Volume.Flags.CLONING))
                {
                    if (success)
                    {
                        vlm.getFlags().enableFlags(Volume.Flags.CLONING_FINISHED);
                        errorReporter.logTrace("Cloning finished with success on %s/%d from %s",
                            rscName, vlmNr, currentPeer.getNode().getName());
                        ResourceDataUtils.recalculateVolatileRscData(ctrlRscLayerDataFactory, rsc);
                    }
                    else
                    {
                        rscDfn.getFlags().enableFlags(ResourceDefinition.Flags.FAILED);
                        // We set the DELETE flag here, to prevent undeletable/broken resources
                        // currently we don't have a good way to ignore layers, but ensure to be able to delete storage
                        // layer volumes, so DELETE is currently our only choice
                        vlm.getFlags().enableFlags(Volume.Flags.DELETE);
                        errorReporter.logError("Error cloning Volume %s/%d on node %s",
                            rscName, vlmNr, currentPeer.getNode().getName());
                    }
                }
                else
                {
                    errorReporter.logError("Clone update for non cloning Volume: %s/%d from node %s",
                        rscName, vlmNr, currentPeer.getNode().getName());
                    throw new ImplementationError(
                        String.format("Clone update for non cloning Volume: %s/%d from node %s",
                            rscName, vlmNr, currentPeer.getNode().getName()));
                }
            }

            // if failed never finish
            boolean allCloned = !rscDfn.getFlags().isSet(ResourceDefinition.Flags.FAILED);
            final Collection<Resource> resources = rscDfn.streamResource().collect(Collectors.toList());
            if (allCloned)
            {
                for (Resource rsc : resources)
                {
                    for (Volume vlm : rsc.streamVolumes().collect(Collectors.toList()))
                    {
                        if (!vlm.getFlags().isSet(Volume.Flags.CLONING_FINISHED))
                        {
                            allCloned = false;
                            break;
                        }
                    }
                    if (!allCloned)
                    {
                        break;
                    }
                }
            }

            if (allCloned)
            {
                for (Resource rsc : resources)
                {
                    for (Volume vlm : rsc.streamVolumes().collect(Collectors.toList()))
                    {
                        vlm.getFlags().disableFlags(Volume.Flags.CLONING);
                        // diskless resources don't clone, so we cleanup the flag after cloning is done
                        if (rsc.isDiskless())
                        {
                            vlm.getFlags().disableFlags(Volume.Flags.CLONING_FINISHED);
                        }
                    }
                    ResourceDataUtils.recalculateVolatileRscData(ctrlRscLayerDataFactory, rsc);
                }

                ctrlTransactionHelper.commit();

                ResourceName resourceName = new ResourceName(rscName);
                flux = ctrlSatelliteUpdateCaller.updateSatellites(rscDfn, Flux.empty())
                    .transform(updateResponses -> CtrlResponseUtils.combineResponses(
                        errorReporter,
                        updateResponses,
                        resourceName,
                        Collections.emptyList(),
                        "Update RscDfn {1} on {0}",
                        "Notified {0} that {1} is being updated on Node(s)"
                        )
                    )
                    .concatWith(resizeAfterClone(resourceName))
                    .concatWith(ctrlRscAutoBalanceHelper.balanceAfterOperation(
                        rscDfn, ApiConsts.KEY_BALANCE_AFTER_CLONE, ApiConsts.NAMESPC_CLONE)
                    ).concatWith(
                        scopeRunner.fluxInTransactionalScope(
                            "Finish clone update unset CLONING",
                            lockGuardFactory.create()
                                .write(LockGuardFactory.LockObj.RSC_DFN_MAP).buildDeferred(),
                            () -> disableCloningFlagInTransaction(rscDfn)
                        )
                    );
            }
            else
            {
                ctrlTransactionHelper.commit();
            }
        }
        catch (ValueOutOfRangeException | DatabaseException | InvalidNameException exc)
        {
            throw new ImplementationError(exc);
        }

        return flux;
    }

    /**
     * Grows the cloned volume definitions to the sizes requested in the clone request (persisted as
     * pending-resize props at clone start). Runs after the clone replicas were brought up but before an
     * optional BalanceAfterClone placement, so the resize only involves the (UpToDate by construction)
     * original clone replicas and the balance replica syncs at the final size.
     */
    private Flux<ApiCallRc> resizeAfterClone(ResourceName rscName)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Resize cloned volume definitions",
                lockGuardFactory.create()
                    .read(LockGuardFactory.LockObj.RSC_DFN_MAP)
                    .buildDeferred(),
                () -> resizeAfterCloneInTransaction(rscName)
            );
    }

    private Flux<ApiCallRc> resizeAfterCloneInTransaction(ResourceName rscName)
    {
        ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(rscName);
        Flux<ApiCallRc> flux = Flux.empty();
        try
        {
            Iterator<VolumeDefinition> itVlmDfn = rscDfn.iterateVolumeDfn();
            while (itVlmDfn.hasNext())
            {
                VolumeDefinition vlmDfn = itVlmDfn.next();
                @Nullable String pendingSizeStr = vlmDfn.getProps().getProp(
                    InternalApiConsts.KEY_CLONE_PENDING_RESIZE, InternalApiConsts.NAMESPC_INTERNAL_CLONE);
                // the size check makes re-running this step idempotent (e.g. satellite re-notify)
                if (pendingSizeStr != null && Long.parseLong(pendingSizeStr) > vlmDfn.getVolumeSize())
                {
                    // sequential per volume: the resize state machine updates satellites rscDfn-wide,
                    // concurrent per-volume resizes would interleave the RESIZE/DRBD_RESIZE stages
                    flux = flux.concatWith(
                        ctrlVlmDfnModifyApiCallHandler.resizeVlmDfn(
                            rscDfn.getName(),
                            vlmDfn.getVolumeNumber(),
                            Long.parseLong(pendingSizeStr),
                            false
                        )
                    );
                }
            }
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
        return flux;
    }

    public Flux<ApiCallRc> disableCloningFlagInTransaction(ResourceDefinition rscDfn)
    {
        try
        {
            rscDfn.getFlags().disableFlags(ResourceDefinition.Flags.CLONING);

            Iterator<VolumeDefinition> itVlmDfn = rscDfn.iterateVolumeDfn();
            while (itVlmDfn.hasNext())
            {
                itVlmDfn.next().getProps().removeProp(
                    InternalApiConsts.KEY_CLONE_PENDING_RESIZE, InternalApiConsts.NAMESPC_INTERNAL_CLONE);
            }

            final Set<Resource> resources = rscDfn.streamResource().collect(Collectors.toSet());
            for (Resource rsc : resources)
            {
                ResourceDataUtils.recalculateVolatileRscData(ctrlRscLayerDataFactory, rsc);
            }

            ctrlTransactionHelper.commit();

            return ctrlSatelliteUpdateCaller.updateSatellites(rscDfn, Flux.empty())
                .transform(updateResponses -> CtrlResponseUtils.combineResponses(
                    errorReporter,
                    updateResponses,
                    rscDfn.getName(),
                    Collections.emptyList(),
                    "Update RscDfn {1} on {0}",
                    "Notified {0} that {1} is being updated on Node(s)"
                    )
                )
                .concatWith(ctrlRscCrtHelper.setInitialized(resources));
        }
        catch (DatabaseException | InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
    }

}
