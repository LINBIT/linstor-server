package com.linbit.linstor.core.apicallhandler.controller.internal;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiDataLoader;
import com.linbit.linstor.core.apicallhandler.controller.CtrlPropsHelper;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscCrtApiHelper;
import com.linbit.linstor.core.apicallhandler.controller.CtrlTransactionHelper;
import com.linbit.linstor.core.apicallhandler.controller.utils.ResourceDataUtils;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.resource.CtrlRscLayerDataFactory;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.locks.LockGuard;
import com.linbit.locks.LockGuardFactory;

import static com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller.notConnectedError;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.stream.Collectors;

import reactor.core.publisher.Flux;

public class RscDfnInternalCallHandler
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlPropsHelper ctrlPropsHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final CtrlStltSerializer ctrlStltSerializer;
    private final CtrlSatelliteUpdater ctrlSatelliteUpdater;
    private final CtrlRscCrtApiHelper ctrlRscCrtHelper;
    private final Provider<Peer> peer;
    private final Provider<AccessContext> peerAccCtx;
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;

    private final ReadWriteLock rscDfnMapLock;
    private final CtrlRscLayerDataFactory ctrlRscLayerDataFactory;

    @Inject
    public RscDfnInternalCallHandler(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlPropsHelper ctrlPropsHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        CtrlStltSerializer ctrlStltSerializerRef,
        CtrlSatelliteUpdater ctrlSatelliteUpdaterRef,
        CtrlRscCrtApiHelper crtlRscCrtHelperRef,
        Provider<Peer> peerRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        CtrlRscLayerDataFactory ctrlRscLayerDataFactoryRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlPropsHelper = ctrlPropsHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        ctrlStltSerializer = ctrlStltSerializerRef;
        ctrlSatelliteUpdater = ctrlSatelliteUpdaterRef;
        ctrlRscCrtHelper = crtlRscCrtHelperRef;
        peer = peerRef;
        peerAccCtx = peerAccCtxRef;
        rscDfnMapLock = rscDfnMapLockRef;
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        ctrlRscLayerDataFactory = ctrlRscLayerDataFactoryRef;
    }

    public void handlePrimaryResourceRequest(
        String rscNameStr,
        UUID rscUuid,
        boolean alreadyInitialized
    )
    {
        Peer currentPeer = peer.get();
        try (LockGuard ignored = LockGuard.createLocked(rscDfnMapLock.writeLock()))
        {
            Resource res = ctrlApiDataLoader.loadRsc(currentPeer.getNode().getName().displayValue, rscNameStr, true);
            ResourceDefinition resDfn = res.getDefinition();

            Props resDfnProps = ctrlPropsHelper.getProps(resDfn);
            if (resDfnProps.getProp(InternalApiConsts.PROP_PRIMARY_SET) == null)
            {
                resDfnProps.setProp(
                    InternalApiConsts.PROP_PRIMARY_SET,
                    res.getNode().getName().value
                );

                ctrlTransactionHelper.commit();

                errorReporter.logTrace(
                    "Primary set for " + currentPeer.getNode().getName().getDisplayName() + "; " +
                        " already initialized: " + alreadyInitialized
                );

                ctrlSatelliteUpdater.updateSatellites(resDfn);

                if (!alreadyInitialized)
                {
                    currentPeer.sendMessage(
                        ctrlStltSerializer
                            .onewayBuilder(InternalApiConsts.API_PRIMARY_RSC)
                            .primaryRequest(rscNameStr, res.getUuid().toString(), false)
                            .build()
                    );
                }
            }
        }
        catch (InvalidKeyException | InvalidValueException | AccessDeniedException ignored)
        {
        }
        catch (DatabaseException sqlExc)
        {
            String errorMessage = String.format(
                "A database error occurred while trying to rollback the deletion of " +
                    "resource definition '%s'.",
                rscNameStr
            );
            errorReporter.reportError(
                sqlExc,
                peerAccCtx.get(),
                currentPeer,
                errorMessage
            );
        }
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
        ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(rscName, true);
        try
        {
            rscDfn.getFlags().enableFlags(apiCtx, ResourceDefinition.Flags.FAILED);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
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

        ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(rscName, true);

        try
        {
            {
                Resource rsc = rscDfn.getResource(apiCtx, currentPeer.getNode().getName());
                Volume vlm = rsc.getVolume(new VolumeNumber(vlmNr));
                if (vlm.getFlags().isSet(apiCtx, Volume.Flags.CLONING))
                {
                    if (success)
                    {
                        vlm.getFlags().enableFlags(apiCtx, Volume.Flags.CLONING_FINISHED);
                        errorReporter.logTrace("Cloning finished with success on %s/%d from %s",
                            rscName, vlmNr, currentPeer.getNode().getName());
                        ResourceDataUtils.recalculateVolatileRscData(ctrlRscLayerDataFactory, rsc);
                    }
                    else
                    {
                        rscDfn.getFlags().enableFlags(apiCtx, ResourceDefinition.Flags.FAILED);
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
            boolean allCloned = !rscDfn.getFlags().isSet(apiCtx, ResourceDefinition.Flags.FAILED);
            if (allCloned)
            {
                for (Resource rsc : rscDfn.streamResource(apiCtx).collect(Collectors.toList()))
                {
                    for (Volume vlm : rsc.streamVolumes().collect(Collectors.toList()))
                    {
                        if (!vlm.getFlags().isSet(apiCtx, Volume.Flags.CLONING_FINISHED))
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
                for (Resource rsc : rscDfn.streamResource(apiCtx).collect(Collectors.toList()))
                {
                    for (Volume vlm : rsc.streamVolumes().collect(Collectors.toList()))
                    {
                        vlm.getFlags().disableFlags(apiCtx, Volume.Flags.CLONING);
                    }
                    ResourceDataUtils.recalculateVolatileRscData(ctrlRscLayerDataFactory, rsc);
                }

                ctrlTransactionHelper.commit();

                ResourceName resourceName = new ResourceName(rscName);
                flux = ctrlSatelliteUpdateCaller.updateSatellites(rscDfn, Flux.empty())
                    .transform(updateResponses -> CtrlResponseUtils.combineResponses(
                        updateResponses,
                        resourceName,
                        Collections.emptyList(),
                        "Update RscDfn {1} on {0}",
                        "Notified {0} that {1} is being updated on Node(s)"
                        )
                    ).concatWith(
                        scopeRunner.fluxInTransactionalScope(
                            "Finish clone update unset FORCE METADATA",
                            lockGuardFactory.create()
                                .write(LockGuardFactory.LockObj.RSC_DFN_MAP).buildDeferred(),
                            () -> disableForceMetadataFlagInTransaction(rscDfn)
                        )
                    );
            }
            else
            {
                ctrlTransactionHelper.commit();
            }
        }
        catch (AccessDeniedException | ValueOutOfRangeException | DatabaseException | InvalidNameException exc)
        {
            throw new ImplementationError(exc);
        }

        return flux;
    }

    public Flux<ApiCallRc> disableForceMetadataFlagInTransaction(ResourceDefinition rscDfn)
    {
        try
        {
            rscDfn.getFlags().disableFlags(apiCtx, ResourceDefinition.Flags.CLONING);

            final Set<Resource> resources = rscDfn.streamResource(apiCtx).collect(Collectors.toSet());
            for (Resource rsc : resources)
            {
                ResourceDataUtils.recalculateVolatileRscData(ctrlRscLayerDataFactory, rsc);
            }

            ctrlTransactionHelper.commit();

            return ctrlSatelliteUpdateCaller.updateSatellites(rscDfn, Flux.empty())
                .transform(updateResponses -> CtrlResponseUtils.combineResponses(
                    updateResponses,
                    rscDfn.getName(),
                    Collections.emptyList(),
                    "Update RscDfn {1} on {0}",
                    "Notified {0} that {1} is being updated on Node(s)"
                    )
                )
                .concatWith(ctrlRscCrtHelper.setInitialized(resources));
        }
        catch (AccessDeniedException | DatabaseException exc)
        {
            throw new ImplementationError(exc);
        }
    }
}
