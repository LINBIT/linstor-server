package com.linbit.linstor.core.apicallhandler.controller.internal;

import com.linbit.ImplementationError;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiDataLoader;
import com.linbit.linstor.core.apicallhandler.controller.CtrlPropsHelper;
import com.linbit.linstor.core.apicallhandler.controller.CtrlTransactionHelper;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.objects.VolumeDefinition.Flags;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.locks.LockGuardFactory;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.List;

import reactor.core.publisher.Flux;

@Singleton
public class VlmDfnInternalCallHandler
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;

    private final CtrlPropsHelper ctrlPropsHelper;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;

    private final Provider<Peer> peer;

    @Inject
    public VlmDfnInternalCallHandler(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlPropsHelper ctrlPropsHelperRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        Provider<Peer> peerRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        ctrlPropsHelper = ctrlPropsHelperRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        peer = peerRef;
    }

    /**
     * Triggered by the UpToDate-race-winning satellite once it finished initialization and succeeded with the first
     * <code>drbdadm adjust</code> without using <code>--skip-disk</code>
     */
    public Flux<ApiCallRc> handleNotifySetUpToDateVlm(String rscNameRef, List<Integer> vlmNrListRef)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Handle notification of setting volume(s) UpToDate for: " + rscNameRef + ", volume(s): " + vlmNrListRef,
                lockGuardFactory.create()
                    .write(LockGuardFactory.LockObj.RSC_DFN_MAP, LockGuardFactory.LockObj.NODES_MAP)
                    .buildDeferred(),
                () -> handleNotifySetUpToDateVlmInTransaction(rscNameRef, vlmNrListRef)
            );
    }

    private Flux<ApiCallRc> handleNotifySetUpToDateVlmInTransaction(
        String rscNameRef,
        List<Integer> vlmNrListRef
    )
    {
        Flux<ApiCallRc> ret = Flux.empty();
        Peer currentPeer = peer.get();
        try
        {
            Resource res = ctrlApiDataLoader.loadRsc(currentPeer.getNode().getName().displayValue, rscNameRef, true);
            ResourceDefinition resDfn = res.getResourceDefinition();
            NodeName nodeName = res.getNode().getName();

            for (int vlmNr : vlmNrListRef)
            {
                @Nullable VolumeDefinition vlmDfn = resDfn.getVolumeDfn(apiCtx, new VolumeNumber(vlmNr));
                if (vlmDfn == null)
                {
                    errorReporter.logWarning(
                        "Ignoring notifySetUpToDate for unknown volume definition. " +
                            "Notifying node: %s, Resource: %s, Volume: %d.",
                        nodeName.displayValue,
                        rscNameRef,
                        vlmNr
                    );
                }
                else
                {
                    Props vlmDfnProps = ctrlPropsHelper.getProps(vlmDfn);

                    // double check state
                    @Nullable String upToDateOn = vlmDfnProps.getProp(
                        InternalApiConsts.KEY_LINSTOR_DRBD_INITIAL_UPTODATE_ON
                    );

                    boolean expectedNode = upToDateOn != null && upToDateOn.equalsIgnoreCase(nodeName.displayValue);
                    StateFlags<Flags> vlmDfnFlags = vlmDfn.getFlags();
                    boolean alreadyInitialzed = vlmDfnFlags.isSet(apiCtx, VolumeDefinition.Flags.DRBD_INITIALIZED);
                    if (!expectedNode || alreadyInitialzed)
                    {
                        errorReporter.logWarning(
                            "Ignoring notifySetUpToDate. Notifying node: %s, Resource: %s, Volume: %d. " +
                                "Expected node: %s",
                            nodeName.displayValue,
                            rscNameRef,
                            vlmNr,
                            upToDateOn
                        );
                    }
                    else
                    {
                        vlmDfnFlags.enableFlags(apiCtx, VolumeDefinition.Flags.DRBD_INITIALIZED);
                    }
                }
            }
            ctrlTransactionHelper.commit();
            errorReporter.logTrace(
                "Selected resource reported to be UpToDate. Node: %s, Resource: %s, Volume(s): %s",
                nodeName.getDisplayName(),
                resDfn.getName().displayValue,
                vlmNrListRef.toString()
            );
            ret = ctrlSatelliteUpdateCaller.updateSatellites(resDfn, null)
                .transform(
                    updateResponses -> CtrlResponseUtils.combineResponses(
                        errorReporter,
                        updateResponses,
                        resDfn.getName(),
                        "Notified node {0} that volume(s) " + rscNameRef + ", " +
                            vlmNrListRef.toString() + " " + (vlmNrListRef.size() == 1 ? "is" : "are") + " initialized"
                    )
                );
        }
        catch (InvalidKeyException | AccessDeniedException | ValueOutOfRangeException implErr)
        {
            throw new ImplementationError(implErr);
        }
        catch (DatabaseException sqlExc)
        {
            String errorMessage = String.format(
                "A database error occurred while trying to progress the UpToDate initialization of " +
                    "volume definition '%s', number %s.",
                rscNameRef,
                vlmNrListRef
            );
            errorReporter.reportError(
                sqlExc,
                peer.get().getAccessContext(),
                currentPeer,
                errorMessage
            );
        }
        return ret;
    }
}
