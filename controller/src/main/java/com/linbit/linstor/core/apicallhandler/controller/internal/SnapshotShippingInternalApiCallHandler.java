package com.linbit.linstor.core.apicallhandler.controller.internal;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiDataLoader;
import com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotDeleteApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlTransactionHelper;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.ResourceConnection;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import reactor.core.publisher.Flux;

@Singleton
public class SnapshotShippingInternalApiCallHandler
{
    private final AccessContext apiCtx;
    private final ScopeRunner scopeRunner;
    private final ErrorReporter errorReporter;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final Provider<Peer> peerProvider;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final DynamicNumberPool snapshotShippingPortPool;
    private final CtrlSnapshotDeleteApiCallHandler snapshotDeleteApiCallHandler;

    @Inject
    public SnapshotShippingInternalApiCallHandler(
        @ApiContext AccessContext apiCtxRef,
        ScopeRunner scopeRunnerRef,
        ErrorReporter errorReporterRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        Provider<Peer> peerProviderRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        @Named(NumberPoolModule.SNAPSHOPT_SHIPPING_PORT_POOL) DynamicNumberPool snapshotShippingPortPoolRef,
        CtrlSnapshotDeleteApiCallHandler snapshotDeleteApiCallHandlerRef
    )
    {
        apiCtx = apiCtxRef;
        scopeRunner = scopeRunnerRef;
        errorReporter = errorReporterRef;
        lockGuardFactory = lockGuardFactoryRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        peerProvider = peerProviderRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        snapshotShippingPortPool = snapshotShippingPortPoolRef;
        snapshotDeleteApiCallHandler = snapshotDeleteApiCallHandlerRef;
    }

    public boolean startShipping(Snapshot targetSnapRef)
    {
        boolean updateSatellite = false;
        SnapshotDefinition snapDfn = targetSnapRef.getSnapshotDefinition();
        try
        {
            String sourceNodeName = snapDfn.getProps(apiCtx).getProp(
                InternalApiConsts.KEY_SNAPSHOT_SHIPPING_SOURCE_NODE
            );
            Snapshot sourceSnap = snapDfn.getSnapshot(apiCtx, new NodeName(sourceNodeName));

            StateFlags<Snapshot.Flags> sourceSnapFlags = sourceSnap.getFlags();
            if (!sourceSnapFlags.isSet(apiCtx, Snapshot.Flags.SHIPPING_SOURCE_START))
            {
                sourceSnapFlags.enableFlags(apiCtx, Snapshot.Flags.SHIPPING_SOURCE_START);
                updateRscConnPropsPreStart(sourceSnap, targetSnapRef);
                updateSatellite = true;
            }
        }
        catch (InvalidKeyException | AccessDeniedException | InvalidNameException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (DatabaseException exc)
        {
            String errorMessage = String.format(
                "A database error occurred while trying to start snapshot shipping of " +
                    "resource: %s",
                targetSnapRef.getResourceName().displayValue
            );
            errorReporter.reportError(
                exc,
                apiCtx,
                peerProvider.get(),
                errorMessage
            );
        }
        return updateSatellite;
    }

    public Flux<ApiCallRc> shippingReceived(String rscNameRef, String snapNameRef, boolean successRef)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Finish received snapshot-shipping",
                lockGuardFactory.create()
                    .read(LockObj.NODES_MAP)
                    .write(LockObj.RSC_DFN_MAP).buildDeferred(),
                () -> shippingReceivedInTransaction(rscNameRef, snapNameRef, successRef)
            );
    }

    private Flux<ApiCallRc> shippingReceivedInTransaction(String rscNameRef, String snapNameRef, boolean successRef)
    {
        Peer stltPeer = peerProvider.get();
        SnapshotDefinition snapDfn = ctrlApiDataLoader.loadSnapshotDfn(rscNameRef, snapNameRef, true);
        Snapshot snapTarget = ctrlApiDataLoader.loadSnapshot(stltPeer.getNode(), snapDfn);
        Snapshot snapSource = getSnapshotShippingSource(snapDfn);

        updateRscConPropsAfterReceived(snapSource, snapTarget);

        snapshotShippingPortPool.deallocate(getPort(snapDfn));
        disableFlags(snapDfn, SnapshotDefinition.Flags.SHIPPING);

        Flux<ApiCallRc> flux;
        if (!successRef)
        {
            ctrlTransactionHelper.commit();

            // deletes the whole snapshotDfn
            flux = snapshotDeleteApiCallHandler.deleteSnapshot(rscNameRef, snapNameRef);
        }
        else
        {
            enableFlags(snapTarget, Snapshot.Flags.DELETE); // just this snapshot, not the whole snapshotDefinition

            ctrlTransactionHelper.commit();

            flux = ctrlSatelliteUpdateCaller.updateSatellites(
                snapDfn,
                CtrlSatelliteUpdateCaller.notConnectedWarn()
            ).transform(
                responses -> CtrlResponseUtils.combineResponses(
                    responses,
                    LinstorParsingUtils.asRscName(rscNameRef),
                    "Deleted snapshot ''" + snapNameRef + "'' of {1} on {0}"
                )
            ).concatWith(deleteSnapshot(rscNameRef, snapNameRef));
        }

        return flux;
    }

    private Snapshot getSnapshotShippingSource(SnapshotDefinition snapDfnRef)
    {
        Snapshot snapShipSource;
        try
        {
            String sourceNodeName = snapDfnRef.getProps(apiCtx).getProp(
                InternalApiConsts.KEY_SNAPSHOT_SHIPPING_SOURCE_NODE
            );
            snapShipSource = snapDfnRef.getSnapshot(apiCtx, new NodeName(sourceNodeName));
        }
        catch (InvalidKeyException | AccessDeniedException | InvalidNameException exc)
        {
            throw new ImplementationError(exc);
        }
        return snapShipSource;
    }

    private void updateRscConnPropsPreStart(Snapshot snapSource, Snapshot snapTarget)
    {
        ResourceConnection rscConn = ctrlApiDataLoader.loadRscConn(
            snapSource.getResourceDefinition().getName(),
            snapSource.getNodeName(),
            snapTarget.getNodeName()
        );
        try
        {
            Props rscConnProps = rscConn.getProps(apiCtx);
            rscConnProps.setProp(
                InternalApiConsts.KEY_SNAPSHOT_SHIPPING_NAME_IN_PROGRESS,
                snapSource.getSnapshotName().displayValue
            );
        }
        catch (AccessDeniedException | InvalidKeyException | InvalidValueException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (DatabaseException exc)
        {
            String errorMessage = String.format(
                "A database error occurred while finishing snapshot shipping of " +
                    "resource: %s",
                snapSource.getResourceName().displayValue
            );
            errorReporter.reportError(
                exc,
                apiCtx,
                peerProvider.get(),
                errorMessage
            );
        }
    }

    private void updateRscConPropsAfterReceived(Snapshot snapSource, Snapshot snapTarget)
    {
        ResourceConnection rscConn = ctrlApiDataLoader.loadRscConn(
            snapSource.getResourceDefinition().getName(),
            snapSource.getNodeName(),
            snapTarget.getNodeName()
        );
        try
        {
            Props rscConnProps = rscConn.getProps(apiCtx);
            rscConnProps.setProp(
                InternalApiConsts.KEY_SNAPSHOT_SHIPPING_NAME_PREV,
                snapSource.getSnapshotName().displayValue
            );
            rscConnProps.removeProp(InternalApiConsts.KEY_SNAPSHOT_SHIPPING_NAME_IN_PROGRESS);
        }
        catch (AccessDeniedException | InvalidKeyException | InvalidValueException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (DatabaseException exc)
        {
            String errorMessage = String.format(
                "A database error occurred while finishing snapshot shipping of " +
                    "resource: %s",
                snapSource.getResourceName().displayValue
            );
            errorReporter.reportError(
                exc,
                apiCtx,
                peerProvider.get(),
                errorMessage
            );
        }
    }

    private void enableFlags(Snapshot snap, Snapshot.Flags... snapshotFlags)
    {
        try
        {
            snap.getFlags().enableFlags(apiCtx, snapshotFlags);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        catch (DatabaseException exc)
        {
            String errorMessage = String.format(
                "A database error occurred while updating snapshot flags of " +
                    "resource: %s, snapshot: %s",
                snap.getResourceName().displayValue,
                snap.getSnapshotName().displayValue
            );
            errorReporter.reportError(
                exc,
                apiCtx,
                peerProvider.get(),
                errorMessage
            );
        }
    }

    private void disableFlags(SnapshotDefinition snapDfnRef, SnapshotDefinition.Flags... flags)
    {
        try
        {
            snapDfnRef.getFlags().disableFlags(apiCtx, flags);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        catch (DatabaseException exc)
        {
            String errorMessage = String.format(
                "A database error occurred while updating snapshot flags of " +
                    "resource: %s, snapshot: %s",
                snapDfnRef.getResourceName().displayValue,
                snapDfnRef.getName().displayValue
            );
            errorReporter.reportError(
                exc,
                apiCtx,
                peerProvider.get(),
                errorMessage
            );
        }
    }

    private int getPort(SnapshotDefinition snapDfn)
    {
        try
        {
            return Integer.parseInt(snapDfn.getProps(apiCtx).getProp(InternalApiConsts.KEY_SNAPSHOT_SHIPPING_PORT));
        }
        catch (NumberFormatException | InvalidKeyException | AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private <T> Flux<T> deleteSnapshot(String rscNameRef, String snapNameRef)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Deleting received and merged snapshot",
                lockGuardFactory.create()
                    .read(LockObj.NODES_MAP)
                    .write(LockObj.RSC_DFN_MAP).buildDeferred(),
                () -> deleteSnapshotInTransaction(rscNameRef, snapNameRef)
            );
    }

    private <T> Flux<T> deleteSnapshotInTransaction(String rscNameRef, String snapNameRef)
    {
        Snapshot snapshot = ctrlApiDataLoader.loadSnapshot(
            peerProvider.get().getNode(),
            ctrlApiDataLoader.loadSnapshotDfn(rscNameRef, snapNameRef, true)
        );
        deleteSnapshotPrivileged(snapshot);
        ctrlTransactionHelper.commit();
        return Flux.empty();
    }

    private void deleteSnapshotPrivileged(Snapshot snapRef)
    {
        try
        {
            snapRef.delete(apiCtx);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (DatabaseException exc)
        {
            String errorMessage = String.format(
                "A database error occurred while trying delete snapshot after shipping shipping of " +
                    "resource: %s",
                snapRef.getResourceName().displayValue
            );
            errorReporter.reportError(
                exc,
                apiCtx,
                peerProvider.get(),
                errorMessage
            );
        }
    }
}
