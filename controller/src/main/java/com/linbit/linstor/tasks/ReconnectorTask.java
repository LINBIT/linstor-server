package com.linbit.linstor.tasks;

import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.core.CtrlAuthenticator;
import com.linbit.linstor.core.SatelliteConnector;
import com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotShippingAbortHandler;
import com.linbit.linstor.core.objects.NetInterface;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
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

import reactor.util.context.Context;

@Singleton
public class ReconnectorTask implements Task
{
    private static final int RECONNECT_SLEEP = 10_000;

    private final Object syncObj = new Object();
    private final HashSet<Peer> peerSet = new HashSet<>();

    private final AccessContext apiCtx;
    private final ErrorReporter errorReporter;
    private PingTask pingTask;
    private final Provider<CtrlAuthenticator> authenticatorProvider;
    private final Provider<SatelliteConnector> satelliteConnector;
    private final TransactionMgrGenerator transactionMgrGenerator;
    private final LinStorScope reconnScope;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlSnapshotShippingAbortHandler snapShipAbortHandler;


    @Inject
    public ReconnectorTask(
        @SystemContext AccessContext apiCtxRef,
        ErrorReporter errorReporterRef,
        Provider<CtrlAuthenticator> authenticatorRef,
        Provider<SatelliteConnector> satelliteConnectorRef,
        TransactionMgrGenerator transactionMgrGeneratorRef,
        LinStorScope reconnScopeRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlSnapshotShippingAbortHandler snapShipAbortHandlerRef
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
                peerSet.add(peer);
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
            if (peerSet.remove(peer) && pingTask != null)
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
            peerSet.remove(peer);
            pingTask.remove(peer);
        }
    }

    @Override
    public long run()
    {
        ArrayList<Peer> localList;
        synchronized (syncObj)
        {
            localList = new ArrayList<>(peerSet);
        }
        for (final Peer peer : localList)
        {
            if (peer.isConnected(false))
            {
                errorReporter.logTrace(
                    peer + " has connected. Removed from reconnectList, added to pingList."
                );
                peerConnected(peer);
            }
            else
            {
                errorReporter.logTrace(
                    "Peer " + peer.getId() + " has not connected yet, retrying connect."
                );
                try
                {
                    Node node = peer.getNode();
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
                            NetInterface currentActiveStltConn = node.getActiveStltConn(peer.getAccessContext());
                            Iterator<NetInterface> netIfIt = node.iterateNetInterfaces(peer.getAccessContext());
                            while (netIfIt.hasNext())
                            {
                                NetInterface netInterface = netIfIt.next();
                                if (!netInterface.equals(currentActiveStltConn) &&
                                    netInterface.isUsableAsStltConn(peer.getAccessContext()))
                                {
                                    errorReporter.logDebug("Setting new active satellite connection: '" +
                                        netInterface.getName() + "'"
                                    );
                                    node.setActiveStltConn(peer.getAccessContext(), netInterface);
                                    break;
                                }
                            }

                            transMgr.commit();
                            synchronized (syncObj)
                            {
                                // add the new peer and remove the old peer to the node
                                peerSet.add(peer.getConnector().reconnect(peer));
                                peerSet.remove(peer);
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
                                peer.getId()
                            );
                        }
                        else
                        {
                            errorReporter.logTrace(
                                "Peer %s's node got deleted, removing from reconnect list",
                                peer.getId()
                            );
                        }

                        synchronized (syncObj)
                        {
                            // no new peer, node is gone. remove the old peer
                            peerSet.remove(peer);
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
}
