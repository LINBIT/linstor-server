package com.linbit.linstor.tasks;

import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.core.CtrlAuthenticator;
import com.linbit.linstor.core.SatelliteConnector;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscAutoHelper;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscAutoHelper.AutoHelperInternalState;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscAutoRePlaceRscHelper;
import com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotShippingAbortHandler;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdater;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.NetInterface;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.repository.NodeRepository;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.satellitestate.SatelliteResourceState;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.tasks.TaskScheduleService.Task;
import com.linbit.linstor.transaction.TransactionException;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.linstor.transaction.manager.TransactionMgrGenerator;
import com.linbit.linstor.transaction.manager.TransactionMgrUtil;
import com.linbit.locks.LockGuard;
import com.linbit.locks.LockGuardFactory;

import static com.linbit.locks.LockGuardFactory.LockObj.CTRL_CONFIG;
import static com.linbit.locks.LockGuardFactory.LockObj.NODES_MAP;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;

import reactor.core.publisher.Flux;
import reactor.util.context.Context;
import reactor.util.function.Tuple2;

@Singleton
public class ReconnectorTask implements Task
{
    private static final int RECONNECT_SLEEP = 10_000;

    private final Object syncObj = new Object();

    private final AccessContext apiCtx;
    private final HashSet<ReconnectConfig> peerSet = new HashSet<>();
    private final ErrorReporter errorReporter;
    private PingTask pingTask;
    private final Provider<CtrlAuthenticator> authenticatorProvider;
    private final Provider<SatelliteConnector> satelliteConnector;
    private final TransactionMgrGenerator transactionMgrGenerator;
    private final LinStorScope reconnScope;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlSnapshotShippingAbortHandler snapShipAbortHandler;
    private final SystemConfRepository systemConfRepo;
    private final Provider<CtrlSatelliteUpdateCaller> ctrlStltUpdateCaller;
    private final Provider<CtrlRscAutoRePlaceRscHelper> autoRePlaceRscHelper;
    private final NodeRepository nodeRepository;

    @Inject
    public ReconnectorTask(
        @SystemContext AccessContext apiCtxRef,
        ErrorReporter errorReporterRef,
        Provider<CtrlAuthenticator> authenticatorRef,
        Provider<SatelliteConnector> satelliteConnectorRef,
        TransactionMgrGenerator transactionMgrGeneratorRef,
        LinStorScope reconnScopeRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlSnapshotShippingAbortHandler snapShipAbortHandlerRef,
        SystemConfRepository systemConfRepoRef,
        Provider<CtrlSatelliteUpdateCaller> ctlrStltUpdateCallerRef,
        Provider<CtrlRscAutoRePlaceRscHelper> autoRePlaceRscHelperRef,
        NodeRepository nodeRepositoryRef
    )
    {
        apiCtx = apiCtxRef;
        errorReporter = errorReporterRef;
        authenticatorProvider = authenticatorRef;
        satelliteConnector = satelliteConnectorRef;
        reconnScope = reconnScopeRef;
        lockGuardFactory = lockGuardFactoryRef;
        transactionMgrGenerator = transactionMgrGeneratorRef;
        snapShipAbortHandler = snapShipAbortHandlerRef;
        systemConfRepo = systemConfRepoRef;
        ctrlStltUpdateCaller = ctlrStltUpdateCallerRef;
        autoRePlaceRscHelper = autoRePlaceRscHelperRef;
        nodeRepository = nodeRepositoryRef;
    }

    void setPingTask(PingTask pingTaskRef)
    {
        pingTask = pingTaskRef;
    }

    public void add(Peer peer, boolean authenticateImmediately)
    {
        add(peer, authenticateImmediately, false);
    }

    public void add(Peer peer, boolean authenticateImmediately, boolean abortSnapshotShippings)
    {
        boolean sendAuthentication = false;
        synchronized (syncObj)
        {
            if (authenticateImmediately && peer.isConnected(false))
            {
                // no locks needed
                sendAuthentication = true;
                pingTask.add(peer);
            }
            else
            {
                peerSet.add(new ReconnectConfig(peer, drbdConnectionsOk(peer)));
            }
        }

        if (abortSnapshotShippings)
        {
            /*
             * FIXME NEEDS PROPER FIX
             *
             * quick and dirty fix. this method could be called by changing the active
             * netif of a node. in this case, the caller is already in a scoped-transaction
             * which means that the above .subscribe will complain about
             *
             * The current scope has already been entered
             *
             * the only way I can think of now would require some rewriting of many method-signatures
             * converting them from non-flux to flux returning methods.
             */
            snapShipAbortHandler.abortSnapshotShippingPrivileged(peer.getNode())
                .subscriberContext(
                    Context.of(
                        ApiModule.API_CALL_NAME,
                        "Abort currently shipped snapshots",
                        AccessContext.class,
                        apiCtx,
                        Peer.class,
                        peer
                    )
                )
                .subscribe();
        }

        if (sendAuthentication)
        {
            /*
             * DO NOT call this method while locking syncObj!
             */

            // no locks needed
            authenticatorProvider.get().sendAuthentication(peer);

            // do not add peer to ping task, as that is done when the satellite
            // authenticated successfully.
        }
    }

    public void peerConnected(Peer peer)
    {
        boolean sendAuthentication = false;
        synchronized (syncObj)
        {
            Object removed = null;
            ReconnectConfig toRemove = null;
            for (ReconnectConfig config : peerSet)
            {
                if (config.peer == peer)
                {
                    toRemove = config;
                }
            }
            if (toRemove != null)
            {
                removed = peerSet.remove(toRemove);
            }
            if (removed != null && pingTask != null)
            {
                sendAuthentication = true;
            }
        }
        if (sendAuthentication)
        {
            /*
             * DO NOT call this method while locking syncObj!
             */

            // no locks needed
            authenticatorProvider.get().sendAuthentication(peer);

            // do not add peer to ping task, as that is done when the satellite
            // authenticated successfully.
        }
    }

    public void removePeer(Peer peer)
    {
        synchronized (syncObj)
        {
            ReconnectConfig toRemove = null;
            for (ReconnectConfig config : peerSet)
            {
                if (config.peer.equals(peer))
                {
                    toRemove = config;
                }
            }
            if (toRemove != null)
            {
                peerSet.remove(toRemove);
            }
            pingTask.remove(peer);
        }
    }

    @Override
    public long run()
    {
        ArrayList<ReconnectConfig> localList;
        synchronized (syncObj)
        {
            localList = getFailedPeers();
        }
        for (final ReconnectConfig config : localList)
        {
            if (config.peer.isConnected(false))
            {
                errorReporter.logTrace(
                    config.peer + " has connected. Removed from reconnectList, added to pingList."
                );
                peerConnected(config.peer);
            }
            else
            {
                errorReporter.logTrace(
                    "Peer " + config.peer.getId() + " has not connected yet, retrying connect."
                );
                try
                {
                    Node node = config.peer.getNode();
                    if (node != null && !node.isDeleted())
                    {
                        boolean hasEnteredScope = false;
                        TransactionMgr transMgr = null;
                        try (LockGuard lockGuard = lockGuardFactory
                            .create()
                            .read(CTRL_CONFIG)
                            .write(NODES_MAP)
                            .build()
                        )
                        {
                            reconnScope.enter();
                            hasEnteredScope = true;
                            transMgr = transactionMgrGenerator.startTransaction();
                            TransactionMgrUtil.seedTransactionMgr(reconnScope, transMgr);

                            // look for another netIf configured as satellite connection and set it as active
                            setNetIf(node, config);

                            transMgr.commit();
                            synchronized (syncObj)
                            {
                                peerSet.add(config.newPeer(config.peer.getConnector().reconnect(config.peer)));
                            }
                        }
                        catch (AccessDeniedException | DatabaseException exc)
                        {
                            errorReporter.logError(exc.getMessage());
                        }
                        finally
                        {
                            if (hasEnteredScope)
                            {
                                reconnScope.exit();
                            }
                            if (transMgr != null)
                            {

                                try
                                {
                                    transMgr.rollback();
                                }
                                catch (TransactionException exc)
                                {
                                    errorReporter.reportError(exc);
                                }
                                transMgr.returnConnection();
                            }
                        }
                    }
                    else
                    {
                        if (node == null)
                        {
                            errorReporter.logTrace(
                                "Peer %s's node is null (possibly rollbacked), removing from reconnect list",
                                config.peer.getId()
                            );
                        }
                        else
                        {
                            errorReporter.logTrace(
                                "Peer %s's node got deleted, removing from reconnect list",
                                config.peer.getId()
                            );
                        }
                    }
                }
                catch (IOException ioExc)
                {
                    // TODO: detailed error reporting
                    errorReporter.reportError(ioExc);
                }
            }
        }
        return RECONNECT_SLEEP;
    }

    private void setNetIf(Node node, ReconnectConfig config) throws AccessDeniedException, DatabaseException
    {
        NetInterface currentActiveStltConn = node
            .getActiveStltConn(config.peer.getAccessContext());
        Iterator<NetInterface> netIfIt = node
            .iterateNetInterfaces(config.peer.getAccessContext());
        NetInterface firstPossible = null;
        boolean setIf = false;
        while (netIfIt.hasNext())
        {
            NetInterface netInterface = netIfIt.next();
            if (netInterface.isUsableAsStltConn(config.peer.getAccessContext()))
            {
                // NetIf usable as StltConn
                if (!setIf && firstPossible == null && !netInterface.equals(currentActiveStltConn))
                {
                    // current connection not found yet, set default if none found
                    firstPossible = netInterface;
                }
                else if (setIf)
                {
                    // already after current connection, set new connection
                    errorReporter.logDebug(
                        "Setting new active satellite connection: '" +
                            netInterface.getName() + "' " +
                            netInterface.getAddress(config.peer.getAccessContext()).getAddress()
                    );
                    node.setActiveStltConn(config.peer.getAccessContext(), netInterface);
                    break;
                }
            }
            if (netInterface.equals(currentActiveStltConn))
            {
                // current connection found, allow new connection after this
                setIf = true;
            }
        }
        if (currentActiveStltConn.equals(node.getActiveStltConn(config.peer.getAccessContext())))
        {
            // current connection not found, use default
            if (firstPossible != null)
            {
                errorReporter.logDebug(
                    "Setting new active satellite connection: '" +
                        firstPossible.getName() + "' " +
                        firstPossible.getAddress(config.peer.getAccessContext()).getAddress()
                );
                node.setActiveStltConn(config.peer.getAccessContext(), firstPossible);
            }
            // else do nothing and reuse current connection, since no other connection is valid
        }
    }

    private ArrayList<ReconnectConfig> getFailedPeers()
    {
        ArrayList<ReconnectConfig> retry = new ArrayList<>();
        PriorityProps props;
        Long timeout;
        for (ReconnectConfig config : peerSet)
        {
            try
            {
                boolean drbdOkNew = drbdConnectionsOk(config.peer);
                props = new PriorityProps(
                    config.peer.getNode().getProps(config.peer.getAccessContext()),
                    systemConfRepo.getCtrlConfForView(config.peer.getAccessContext())
                );
                timeout = Long.parseLong(
                    props.getProp(
                        ApiConsts.KEY_NODE_MAX_OFFLINE_TIME,
                        ApiConsts.NAMESPC_NODE,
                        "60" // 1 hour
                    )
                ) * 60 * 100; // to milliseconds
                if (config.drbdOk != drbdOkNew)
                {
                    config.drbdOk = drbdOkNew;
                    if (!config.drbdOk)
                    {
                        config.offlineSince = System.currentTimeMillis();
                    }
                }
                if ((!config.drbdOk && System.currentTimeMillis() < config.offlineSince + timeout) || (config.drbdOk))
                {
                    retry.add(config);
                }
                else
                {
                    retry.add(config);
                    int numLost = peerSet.size();
                    int maxPercentLost = Integer.parseInt(
                        props.getProp(ApiConsts.KEY_MAX_DISCONNECTED_NODES, ApiConsts.NAMESPC_NODE, "10")
                    );
                    if (maxPercentLost < 0 || maxPercentLost > 100)
                    {
                        maxPercentLost = 10;
                    }
                    int numNodes = nodeRepository.getMapForView(apiCtx).size();
                    int maxLost = Math.round(maxPercentLost * numNodes / 100);
                    if (numLost < maxLost && !config.peer.getNode().getFlags().isSet(apiCtx, Node.Flags.DEAD))
                    {
                        errorReporter.logTrace(
                            config.peer + " has been offline for too long, relocation of resources started."
                        );
                        Node node = config.peer.getNode();
                        node.markDead(apiCtx);
                        Flux<Tuple2<NodeName, Flux<ApiCallRc>>> flux = ctrlStltUpdateCaller.get().updateSatellites(
                            node.getUuid(),
                            node.getName(),
                            CtrlSatelliteUpdater.findNodesToContact(apiCtx, node)
                        );
                        CtrlRscAutoHelper.AutoHelperInternalState autoState = new AutoHelperInternalState();
                        autoState.additionalFluxList.add(
                            flux.transform(tuple ->
                            {
                                return Flux.<ApiCallRc> empty();
                            })
                        );
                        for (Resource res : node.streamResources(apiCtx).collect(Collectors.toList()))
                        {
                            res.markDeleted(apiCtx);
                            autoRePlaceRscHelper.get().addNeedRePlaceRsc(res);
                            autoRePlaceRscHelper.get().manage(new ApiCallRcImpl(), res.getDefinition(), autoState);
                        }
                        Flux.concat(autoState.additionalFluxList).subscribe();
                    }
                }
            }
            catch (AccessDeniedException exc)
            {
                errorReporter.reportError(exc);
            }
            catch (DatabaseException exc)
            {
                errorReporter.reportError(exc);
            }
        }
        return retry;
    }

    private boolean drbdConnectionsOk(Peer peer)
    {
        Map<ResourceName, SatelliteResourceState> resStates = peer.getSatelliteState().getResourceStates();
        if (resStates.isEmpty())
        {
            return false;
        }
        boolean drbdOk = true;
        for (SatelliteResourceState state : resStates.values())
        {
            if (state.getConnectionStates().isEmpty())
            {
                drbdOk = false;
            }
        }
        return drbdOk;
    }

    public void startReconnecting(Collection<Node> nodes, AccessContext initCtx)
    {
        /*
         * We need this method so that all nodes are added starting connecting while having
         * this syncObj. If we would give up the syncObj between two startConnecting calls
         * we might run into a deadlock where one thread tries to connect (awaits authentication)
         * and another thread tries to start connecting the next node.
         */
        synchronized (syncObj)
        {
            SatelliteConnector stltConnector = satelliteConnector.get();
            for (Node node : nodes)
            {
                errorReporter.logDebug("Reconnecting to node '" + node.getName() + "'.");
                stltConnector.startConnecting(node, initCtx);
            }
        }
    }

    private static class ReconnectConfig
    {
        private Peer peer;
        private long offlineSince;
        private boolean drbdOk;

        private ReconnectConfig(Peer peerRef, boolean drbdRef)
        {
            peer = peerRef;
            drbdOk = drbdRef;
            offlineSince = System.currentTimeMillis();
        }

        public ReconnectConfig newPeer(Peer peerRef)
        {
            peer = peerRef;
            return this;
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + (int) (offlineSince ^ (offlineSince >>> 32));
            result = prime * result + ((peer == null) ? 0 : peer.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            ReconnectConfig other = (ReconnectConfig) obj;
            if (offlineSince != other.offlineSince)
                return false;
            if (peer == null)
            {
                if (other.peer != null)
                    return false;
            }
            else if (!peer.equals(other.peer))
                return false;
            return true;
        }
    }
}
