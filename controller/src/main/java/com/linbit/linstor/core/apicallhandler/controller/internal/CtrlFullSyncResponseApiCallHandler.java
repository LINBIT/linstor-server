package com.linbit.linstor.core.apicallhandler.controller.internal;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiConsts.ConnectionStatus;
import com.linbit.linstor.core.BackupInfoManager;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlNodeApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRemoteApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlSatelliteConnectionNotifier;
import com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotDeleteApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlTransactionHelper;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.tasks.ReconnectorTask;
import com.linbit.locks.LockGuard;
import com.linbit.utils.PairNonNull;

import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;

import org.slf4j.MDC;
import reactor.core.publisher.Flux;

@Singleton
public class CtrlFullSyncResponseApiCallHandler
{
    private static final String PROP_NAMESPACE_STLT = "Satellite/";

    private final ErrorReporter errorReporter;
    private final ScopeRunner scopeRunner;
    private final CtrlSatelliteConnectionNotifier ctrlSatelliteConnectionNotifier;
    private final ReadWriteLock nodesMapLock;
    private final ReadWriteLock rscDfnMapLock;
    private final CtrlSnapshotDeleteApiCallHandler ctrlSnapDelApiCallHandler;
    private final BackupInfoManager backupInfoMgr;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlRemoteApiCallHandler ctrlRemoteApiCallHandler;
    private final Provider<ReconnectorTask> reconnectorTaskProvider;

    public static class FullSyncSuccessContext
    {
        private final Peer peer;
        private final Map<String, String> stltPropsToSet;
        private final Collection<String> stltPropKeysToDelete;
        private final Collection<String> stltPropNamespacesToDelete;

        public FullSyncSuccessContext(
            Peer peerRef,
            Map<String, String> stltPropsToSetRef,
            Collection<String> stltPropKeysToDeleteRef,
            Collection<String> stltPropNamespacesToDeleteRef
        )
        {
            peer = peerRef;
            stltPropsToSet = stltPropsToSetRef;
            stltPropKeysToDelete = stltPropKeysToDeleteRef;
            stltPropNamespacesToDelete = stltPropNamespacesToDeleteRef;
        }
    }

    @Inject
    public CtrlFullSyncResponseApiCallHandler(
        ErrorReporter errorReporterRef,
        ScopeRunner scopeRunnerRef,
        CtrlSatelliteConnectionNotifier ctrlSatelliteConnectionNotifierRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        CtrlSnapshotDeleteApiCallHandler ctrlSnapDelApiCallHandlerRef,
        BackupInfoManager backupInfoMgrRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlRemoteApiCallHandler ctrlRemoteApiCallHandlerRef,
        Provider<ReconnectorTask> reconnectorTaskProviderRef
    )
    {
        errorReporter = errorReporterRef;
        scopeRunner = scopeRunnerRef;
        ctrlSatelliteConnectionNotifier = ctrlSatelliteConnectionNotifierRef;
        nodesMapLock = nodesMapLockRef;
        rscDfnMapLock = rscDfnMapLockRef;
        ctrlSnapDelApiCallHandler = ctrlSnapDelApiCallHandlerRef;
        backupInfoMgr = backupInfoMgrRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlRemoteApiCallHandler = ctrlRemoteApiCallHandlerRef;
        reconnectorTaskProvider = reconnectorTaskProviderRef;
    }

    /**
     * This method should be called when the satellite successfully applied its FullSync.
     *
     * <ul>
     * <li>Calls {@link CtrlSatelliteConnectionNotifier#resourceConnected(Resource, ResponseContext)} for every
     *     resource of the given node</li>
     * <li>Cleans up all snapshots and (temporary) remotes from failed backups of the given node</li>
     * <li>Merges node properties within the "Satellite/" namespace that the satellite told us in its
     *     FullSyncResponse</li>
     * <li>Sets the node to {@link com.linbit.linstor.api.ApiConsts.ConnectionStatus#ONLINE}</li>
     * </ul>
     *
     * @return A merged Flux<?> continuing the "resource connected", "cleanup backups" and "cleanup remotes".
     */
    public Flux<?> fullSyncSuccess(FullSyncSuccessContext fullSyncSuccessCtx, @Nullable ResponseContext context)
    {
        final ResponseContext responseCtx;
        if (context == null)
        {
            responseCtx = CtrlNodeApiCallHandler.makeNodeContext(
                ApiOperation.makeCreateOperation(),
                fullSyncSuccessCtx.peer.getNode().getName().displayValue
            );
        }
        else
        {
            responseCtx = context;
        }
        return scopeRunner.fluxInTransactionalScope(
            "Handle full sync success",
            LockGuard.createDeferred(
                nodesMapLock.writeLock(),
                rscDfnMapLock.readLock()
            ),
            () -> fullSyncSuccessInScope(fullSyncSuccessCtx, responseCtx)
        );
    }

    private Flux<?> fullSyncSuccessInScope(FullSyncSuccessContext fullSyncSuccessCtxRef, ResponseContext responseCtxRef)
    {
        final Peer satellitePeer = fullSyncSuccessCtxRef.peer;
        final Node localNode = satellitePeer.getNode();

        List<Flux<?>> fluxes = new ArrayList<>();

        try
        {
            PairNonNull<Set<SnapshotDefinition>, Set<RemoteName>> objsToDel = backupInfoMgr.removeAllRestoreEntries(
                localNode
            );
            for (SnapshotDefinition snapDfn : objsToDel.objA)
            {
                fluxes.add(
                    ctrlSnapDelApiCallHandler.deleteSnapshot(
                        snapDfn.getResourceName(),
                        snapDfn.getName(),
                        null
                    )
                );
            }

            mergeNodeProps(fullSyncSuccessCtxRef);

            fluxes.add(ctrlRemoteApiCallHandler.cleanupRemotesIfNeeded(objsToDel.objB));
            ctrlTransactionHelper.commit();

            satellitePeer.setConnectionStatus(ApiConsts.ConnectionStatus.ONLINE);

            Iterator<Resource> localRscIter = localNode.iterateResources();
            while (localRscIter.hasNext())
            {
                Resource localRsc = localRscIter.next();
                fluxes.add(ctrlSatelliteConnectionNotifier.resourceConnected(localRsc, responseCtxRef));
            }

            satellitePeer.fullSyncApplied();
        }
        catch (DatabaseException dbExc)
        {
            satellitePeer.setConnectionStatus(ApiConsts.ConnectionStatus.FULL_SYNC_FAILED);
            satellitePeer.fullSyncFailed();

            throw new ApiDatabaseException(dbExc);
        }
        catch (InvalidValueException exc)
        {
            throw new ImplementationError(exc);
        }

        return Flux.merge(fluxes);
    }

    private void mergeNodeProps(FullSyncSuccessContext ctxRef)
        throws InvalidValueException, DatabaseException
    {
        Props nodeProps = ctxRef.peer.getNode().getProps();
        for (Map.Entry<String, String> entry : ctxRef.stltPropsToSet.entrySet())
        {
            String key = entry.getKey();
            if (key.startsWith(PROP_NAMESPACE_STLT))
            {
                nodeProps.setProp(key, entry.getValue());
            }
        }
        for (String propKeyToDelete : ctxRef.stltPropKeysToDelete)
        {
            if (propKeyToDelete.startsWith(PROP_NAMESPACE_STLT))
            {
                nodeProps.removeProp(propKeyToDelete);
            }
        }
        for (String propNamespaceToDelete : ctxRef.stltPropNamespacesToDelete)
        {
            if (propNamespaceToDelete.startsWith(PROP_NAMESPACE_STLT))
            {
                nodeProps.removeNamespace(propNamespaceToDelete);
            }
        }
    }

    /**
     * This method should be called when the satellite could not properly apply its FullSync.
     *
     * <ul>
     * <li>Calls {@link Peer#fullSyncFailed(com.linbit.linstor.api.ApiConsts.ConnectionStatus)} with
     *     the given {@link ConnectionStatus}</li>
     * </ul>
     */
    public Flux<?> fullSyncFailed(Peer satellitePeerRef, ApiConsts.ConnectionStatus connectionStatusRef)
    {
        return scopeRunner.fluxInTransactionlessScope(
            "Handle full sync failed",
            LockGuard.createDeferred(
                nodesMapLock.writeLock(),
                rscDfnMapLock.readLock()
            ),
            () -> fullSyncFailedInScope(satellitePeerRef, connectionStatusRef),
            MDC.getCopyOfContextMap()
        );
    }

    private Flux<?> fullSyncFailedInScope(Peer satellitePeerRef, ApiConsts.ConnectionStatus connectionStatusRef)
    {
        satellitePeerRef.fullSyncFailed(connectionStatusRef);
        return Flux.empty();
    }

    /**
     * This method should be called when the satellite refused a FullSync because it expects a FullSync based
     * on a newer fullSyncId (i.e. from a more recent authentication - see "double reconnect" race).
     *
     * <p>
     * Unlike {@link #fullSyncFailed(Peer, ApiConsts.ConnectionStatus)} this is not a permanent failure: the
     * handshake simply has to be restarted. If this connection is still the node's current connection (and no
     * newer FullSync is already in flight on it), the connection is closed and handed to the
     * {@link ReconnectorTask}, which re-establishes the connection and re-runs the Auth + FullSync handshake.
     * </p>
     */
    public Flux<?> fullSyncOutdated(Peer satellitePeerRef, long refusedFullSyncIdRef)
    {
        return scopeRunner.fluxInTransactionlessScope(
            "Handle outdated full sync",
            LockGuard.createDeferred(
                nodesMapLock.writeLock(),
                rscDfnMapLock.readLock()
            ),
            () -> fullSyncOutdatedInScope(satellitePeerRef, refusedFullSyncIdRef),
            MDC.getCopyOfContextMap()
        );
    }

    private Flux<?> fullSyncOutdatedInScope(Peer satellitePeerRef, long refusedFullSyncIdRef)
    {
        if (satellitePeerRef.getFullSyncId() != refusedFullSyncIdRef)
        {
            // a newer FullSync was already sent over this connection - its response will decide the outcome
            errorReporter.logInfo(
                "Satellite %s refused the outdated full sync %d, but a newer full sync (%d) is already in " +
                    "flight - awaiting its response",
                satellitePeerRef,
                refusedFullSyncIdRef,
                satellitePeerRef.getFullSyncId()
            );
        }
        else
        {
            // make sure no further updates are sent over this connection based on the refused fullSyncId
            satellitePeerRef.fullSyncFailed(ApiConsts.ConnectionStatus.OFFLINE);

            @Nullable Node node = satellitePeerRef.getNode();
            if (node != null && !node.isDeleted() && node.getPeer().equals(satellitePeerRef))
            {
                errorReporter.logWarning(
                    "Satellite %s refused the full sync %d as outdated. Restarting the handshake by " +
                        "reconnecting",
                    satellitePeerRef,
                    refusedFullSyncIdRef
                );
                satellitePeerRef.closeConnection();
                reconnectorTaskProvider.get().add(satellitePeerRef, false);
            }
            else
            {
                // this connection was already replaced; the newer connection runs its own handshake
                errorReporter.logInfo(
                    "Satellite %s refused the full sync %d as outdated on an already replaced connection - " +
                        "closing it",
                    satellitePeerRef,
                    refusedFullSyncIdRef
                );
                satellitePeerRef.closeConnection();
            }
        }
        return Flux.empty();
    }
}
