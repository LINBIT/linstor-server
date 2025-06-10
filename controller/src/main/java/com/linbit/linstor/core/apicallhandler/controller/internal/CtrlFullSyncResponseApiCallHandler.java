package com.linbit.linstor.core.apicallhandler.controller.internal;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.ApiContext;
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
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.locks.LockGuard;
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

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

    private final AccessContext apiCtx;
    private final ScopeRunner scopeRunner;
    private final CtrlSatelliteConnectionNotifier ctrlSatelliteConnectionNotifier;
    private final ReadWriteLock nodesMapLock;
    private final ReadWriteLock rscDfnMapLock;
    private final CtrlSnapshotDeleteApiCallHandler ctrlSnapDelApiCallHandler;
    private final BackupInfoManager backupInfoMgr;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlRemoteApiCallHandler ctrlRemoteApiCallHandler;

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
        @ApiContext AccessContext apiCtxRef,
        ScopeRunner scopeRunnerRef,
        CtrlSatelliteConnectionNotifier ctrlSatelliteConnectionNotifierRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        CtrlSnapshotDeleteApiCallHandler ctrlSnapDelApiCallHandlerRef,
        BackupInfoManager backupInfoMgrRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlRemoteApiCallHandler ctrlRemoteApiCallHandlerRef
    )
    {
        apiCtx = apiCtxRef;
        scopeRunner = scopeRunnerRef;
        ctrlSatelliteConnectionNotifier = ctrlSatelliteConnectionNotifierRef;
        nodesMapLock = nodesMapLockRef;
        rscDfnMapLock = rscDfnMapLockRef;
        ctrlSnapDelApiCallHandler = ctrlSnapDelApiCallHandlerRef;
        backupInfoMgr = backupInfoMgrRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlRemoteApiCallHandler = ctrlRemoteApiCallHandlerRef;
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
    public Flux<?> fullSyncSuccess(FullSyncSuccessContext fullSyncSuccessCtx, ResponseContext context)
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
            Pair<Set<SnapshotDefinition>, Set<RemoteName>> objsToDel = backupInfoMgr.removeAllRestoreEntries(
                apiCtx,
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

            Iterator<Resource> localRscIter = localNode.iterateResources(apiCtx);
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
        catch (AccessDeniedException | InvalidValueException exc)
        {
            throw new ImplementationError(exc);
        }

        return Flux.merge(fluxes);
    }

    private void mergeNodeProps(FullSyncSuccessContext ctxRef)
        throws AccessDeniedException, InvalidValueException, DatabaseException
    {
        Props nodeProps = ctxRef.peer.getNode().getProps(apiCtx);
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
}
