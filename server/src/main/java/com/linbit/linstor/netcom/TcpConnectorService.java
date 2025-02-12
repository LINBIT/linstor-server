package com.linbit.linstor.netcom;

import com.linbit.ErrorCheck;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.SystemServiceStartException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.AccessToDeletedDataException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.NetInterface;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.TcpConnectorPeer.ReadState;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import java.io.IOException;
import java.net.ConnectException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NoRouteToHostException;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.net.UnknownHostException;
import java.nio.channels.AlreadyBoundException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.IllegalBlockingModeException;
import java.nio.channels.IllegalSelectorException;
import java.nio.channels.NotYetBoundException;
import java.nio.channels.NotYetConnectedException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.UnsupportedAddressTypeException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.MDC;
import org.slf4j.event.Level;

import static java.nio.channels.SelectionKey.OP_ACCEPT;
import static java.nio.channels.SelectionKey.OP_CONNECT;
import static java.nio.channels.SelectionKey.OP_READ;
import static java.nio.channels.SelectionKey.OP_WRITE;

/**
 * TCP/IP network communication service
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class TcpConnectorService implements Runnable, TcpConnector
{
    /**
     * Value based on RFC 2474 and RFC 4594
     */
    private static final int IP_DSCP_OAM_LDP = 0x48;

    private static final ServiceName SERVICE_NAME;
    private static final String SERVICE_INFO = "TCP/IP network communications service";

    private final Object syncObj = new Object();

    protected ServiceName serviceInstanceName;

    private static final long REINIT_THROTTLE_TIME = 3000L;

    public static final int DEFAULT_PORT_VALUE = 9977;
    public static final TcpPortNumber DEFAULT_PORT;

    public static final InetAddress DEFAULT_BIND_INET_ADDRESS;
    public static final SocketAddress DEFAULT_BIND_ADDRESS;

    // IPv4 127.0.0.1
    public static final byte[] FALLBACK_LOOPBACK_ADDR = new byte[] {0x7F, 0, 0, 1};
    // IPv4 0.0.0.0
    public static final byte[] FALLBACK_ANY_LOCAL_ADDR = new byte[] {0, 0, 0, 0};

    // Maximum number of connections to accept in one selector iteration
    public static final int MAX_ACCEPT_LOOP = 100;

    protected final ErrorReporter errorReporter;
    protected final CommonSerializer commonSerializer;
    private MessageProcessor msgProcessor;

    // Set by shutdown() to shut down the selector loop
    private AtomicBoolean shutdownFlag;

    // Selector loop thread
    private @Nullable Thread selectorLoopThread;

    private ConnectionObserver connObserver;

    static
    {
        try
        {
            DEFAULT_PORT = new TcpPortNumber(DEFAULT_PORT_VALUE);

            SERVICE_NAME = new ServiceName("NetComService");
        }
        catch (ValueOutOfRangeException valueExc)
        {
            throw new ImplementationError(
                String.format(
                    "%s class default port constant is set to the out-of-range value %d",
                    TcpConnectorService.class.getName(), DEFAULT_PORT_VALUE
                ),
                valueExc
            );
        }
        catch (InvalidNameException nameExc)
        {
            throw new ImplementationError(
                String.format(
                    "%s class contains an invalid name constant",
                    TcpConnectorService.class.getName()
                ),
                nameExc
            );
        }

        // Initialize the default bind address
        {
            final int ipV6Len = 16;
            byte[] defaultIpv6Addr = new byte[ipV6Len];
            Arrays.fill(defaultIpv6Addr, (byte) 0);
            try
            {
                DEFAULT_BIND_INET_ADDRESS = Inet6Address.getByAddress(defaultIpv6Addr);
            }
            catch (UnknownHostException hostExc)
            {
                throw new ImplementationError(
                    String.format(
                        "%s class default bind address constant is set to an illegal value",
                        TcpConnectorService.class.getName()
                    ),
                    hostExc
                );
            }
        }
        DEFAULT_BIND_ADDRESS = new InetSocketAddress(
            DEFAULT_BIND_INET_ADDRESS,
            DEFAULT_PORT.value
        );
    }

    // Address that the server socket will be listening on
    private @Nullable SocketAddress bindAddress;

    // Server socket for accepting incoming connections
    private @Nullable ServerSocketChannel serverSocket;

    // Default access context for a newly connected peer
    protected AccessContext defaultPeerAccCtx;

    // Privileged access context for e.g. setting peer to node
    private final AccessContext privilegedAccCtx;

    // Selector for all connections
    @Nullable Selector serverSelector;

    private final AtomicInteger connCount = new AtomicInteger(0);


    public TcpConnectorService(
        ErrorReporter errorReporterRef,
        CommonSerializer commonSerializerRef,
        MessageProcessor msgProcessorRef,
        @Nullable SocketAddress bindAddressRef,
        AccessContext defaultPeerAccCtxRef,
        AccessContext privilegedAccCtxRef,
        ConnectionObserver connObserverRef
    )
    {
        ErrorCheck.ctorNotNull(TcpConnectorService.class, ErrorReporter.class, errorReporterRef);
        ErrorCheck.ctorNotNull(TcpConnectorService.class, MessageProcessor.class, msgProcessorRef);
        ErrorCheck.ctorNotNull(TcpConnectorService.class, AccessContext.class, defaultPeerAccCtxRef);
        ErrorCheck.ctorNotNull(TcpConnectorService.class, AccessContext.class, privilegedAccCtxRef);
        serviceInstanceName = SERVICE_NAME;

        serverSocket    = null;
        serverSelector  = null;
        errorReporter   = errorReporterRef;
        commonSerializer = commonSerializerRef;
        msgProcessor    = msgProcessorRef;
        // Prevent entering the run() method's selector loop
        // until initialize() has completed
        shutdownFlag    = new AtomicBoolean(true);
        connObserver    = new SafeConnectionObserver(errorReporterRef, connObserverRef);

        defaultPeerAccCtx = defaultPeerAccCtxRef;
        privilegedAccCtx = privilegedAccCtxRef;
        bindAddress = bindAddressRef;
    }

    @Override
    public Peer connect(InetSocketAddress address, Node node) throws IOException
    {
        errorReporter.logInfo("Establishing connection to node '%s' via %s ...", node.getName(), address);
        Selector srvSel = serverSelector;
        Peer peer;
        if (srvSel != null)
        {
            SocketChannel socketChannel = null;
            try
            {
                socketChannel = SocketChannel.open();
                socketChannel.configureBlocking(false);
                socketChannel.socket().setTcpNoDelay(true);
                String peerId = address.getAddress().getHostAddress() + ":" + address.getPort() + "/"
                    + connCount.incrementAndGet();
                SelectionKey connKey;
                synchronized (syncObj)
                {
                    srvSel.wakeup();
                    try
                    {
                        // set IP_TOS before connect so also SYN packet gets prioritized
                        socketChannel.setOption(StandardSocketOptions.IP_TOS, IP_DSCP_OAM_LDP);
                    }
                    catch (IOException ioExc)
                    {
                        // setting IP_TOS is just a "best effort" attempt.
                        errorReporter.logDebug("Failed to set IP_TOS to " + IP_DSCP_OAM_LDP);
                    }
                    boolean connected = socketChannel.connect(address);
                    if (connected)
                    {
                        // if connect is true, we will never receive an OP_CONNECT
                        // even if we register for it.
                        // as the controller does not know about this peer (we didnt return yet)
                        // we will register for no operation.
                        // As soon as the controller tries to send a message, that will trigger the OP_WRITE anyways
                        connKey = socketChannel.register(srvSel, 0);
                    }
                    else
                    {
                        // if connect returns false we will receive OP_CONNECT
                        // and we will need to call the finishConnection()
                        connKey = socketChannel.register(srvSel, OP_CONNECT);
                    }
                    peer = createTcpConnectorPeer(address, peerId, connKey, true, node);
                    connKey.attach(peer);
                    if (connected)
                    {
                        // May throw SSLException
                        peer.connectionEstablished();
                        connObserver.outboundConnectionEstablished(peer);
                    }
                    else
                    {
                        connObserver.outboundConnectionEstablishing(peer);
                    }
                    try
                    {
                        node.setPeer(privilegedAccCtx, peer);
                    }
                    catch (AccessDeniedException accDeniedExc)
                    {
                        throw new ImplementationError(
                            "TcpConnectorService privileged access context not authorized for node.setPeer() " +
                            "called from connect()",
                            accDeniedExc
                        );
                    }
                }
            }
            catch (IOException ioExc)
            {
                try
                {
                    if (socketChannel != null)
                    {
                        socketChannel.close();
                    }
                }
                catch (IOException ignored)
                {
                }
                throw ioExc;
            }
            catch (IllegalBlockingModeException blkModeException)
            {
                throw new IOException(
                    "Connect request failed - Non-blocking I/O mode requested, but not supported"
                );
            }
            catch (IllegalSelectorException | ClosedSelectorException |
                   CancelledKeyException connExc)
            {
                throw new IOException(
                    "Connect request failed - Connector service '" + serviceInstanceName + "' state changed " +
                    "while the operation was in progress"
                );
            }
        }
        else
        {
            throw new IOException(
                "Connect request failed - Connector service '" + serviceInstanceName + "' is stopped"
            );
        }
        return peer;
    }

    @Override
    public Peer reconnect(Peer peer) throws IOException
    {
        peer.closeConnection();

        InetSocketAddress address;

        if (peer.getNode() == null)
        {
            address = peer.getHostAddr();
        }
        else
        {
            NetInterface activeStltConn;
            try
            {
                activeStltConn = peer.getNode().getActiveStltConn(privilegedAccCtx);
                if (activeStltConn == null)
                {
                    // this might happen when the node was rolled back and thus no longer has any content
                    address = peer.getHostAddr();
                }
                else
                {
                    final String host = activeStltConn.getAddress(privilegedAccCtx).getAddress();
                    final int port = activeStltConn.getStltConnPort(privilegedAccCtx).value;
                    address = new InetSocketAddress(host, port);
                }
            }
            catch (AccessDeniedException exc)
            {
                throw new ImplementationError(
                    "Privileged access context not authorized to access node's network interface",
                    exc
                );
            }
            catch (AccessToDeletedDataException ignored)
            {
                // if the netInterface was concurrently deleted, we just ignore this "access to deleted NetIf" exception
                address = peer.getHostAddr(); // use the old peer's address
            }
        }

        return this.connect(address, peer.getNode());
    }

    @Override
    public synchronized void start()
        throws SystemServiceStartException
    {
        if (selectorLoopThread == null)
        {
            try
            {
                initialize();
            }
            catch (IOException ioExc)
            {
                String descriptionText = String.format(
                    "Initialization of the %s service instance '%s' failed.",
                    TcpConnectorService.class.getName(),
                    serviceInstanceName.displayValue
                );
                throw new SystemServiceStartException(
                    descriptionText,
                    // Description
                    descriptionText,
                    // Cause
                    ioExc.getMessage(),
                    // Correction
                    null,
                    // Details
                    null,
                    ioExc,
                    false
                );
            }
            selectorLoopThread = new Thread(this);
            selectorLoopThread.setName(serviceInstanceName.getDisplayName());
            selectorLoopThread.start();
        }
    }

    @Override
    public synchronized void shutdown()
    {
        shutdownFlag.set(true);
        Selector srvSel = serverSelector;
        if (srvSel != null)
        {
            srvSel.wakeup();
        }
    }

    @Override
    public void awaitShutdown(long timeout)
        throws InterruptedException
    {
        Thread joinThr = null;
        synchronized (this)
        {
            joinThr = selectorLoopThread;
        }
        if (joinThr != null)
        {
            joinThr.join(timeout);
        }
    }

    @Override
    public void run()
    {
        // Selector loop
        LinkedList<Peer> peersWithFinishedMessages = new LinkedList<>();
        while (!shutdownFlag.get())
        {
            try (var ignore = MDC.putCloseable(ErrorReporter.LOGID, ErrorReporter.getNewLogId()))
            {
                try
                {
                    if (peersWithFinishedMessages.isEmpty())
                    {
                        // Block until I/O operations are ready to be performed
                        // on at least one of the channels, or until the selection
                        // operation is interrupted (e.g., using wakeup())
                        int selectCount = serverSelector.select();

                        synchronized (syncObj)
                        {
                            // wait for the syncObj to get released

                            // Ensure making some progress in the case that
                            // the blocking select() call is repeatedly interrupted
                            // (e.g., using wakeup()) before having selected any
                            // channels
                            if (selectCount <= 0)
                            {
                                /*
                                 * this selectNow has to be inside the synchronized block as otherwise
                                 * it would be possible that the TcpConnector thread is already past
                                 * the previous .select(), THEN another thread (i.e. reconnector)
                                 * calls .wakeup() which is immediately consumed by the .selectNow()
                                 * BEFORE the thread calling .wakeup() could register some new
                                 * listeners. We suspect that we ended up in a deadlock occasionally
                                 * because of this.
                                 */
                                serverSelector.selectNow();
                            }
                        }

                    }
                    else
                    {

                        ListIterator<Peer> listIterator = peersWithFinishedMessages.listIterator();
                        while (listIterator.hasNext())
                        {
                            try
                            {
                                boolean finished = true;
                                Peer peer = listIterator.next();
                                if (peer.hasNextMsgIn())
                                {
                                    msgProcessor.processMessage(peer.nextCurrentMsgIn(), this, peer);
                                    finished = false;
                                }

                                if (finished)
                                {
                                    listIterator.remove();
                                }
                            }
                            catch (CancelledKeyException ignored)
                            {
                                // Selection key no longer valid
                                // Cleaned up by the next select() or selectNow() operation

                            }
                        }

                        // we tried to process one message from each waiting peer.
                        // now we see if we have new operations (read, write, accept, connect)
                        // if peers still have more messages, they have to wait until the next
                        // loop-cycle (fair scheduling).
                        serverSelector.selectNow();
                    }
                }
                catch (CancelledKeyException ignored)
                {
                    // Selection key no longer valid
                    // Cleaned up by the next select() or selectNow() operation
                }

                onSelectorWakeup();

                Iterator<SelectionKey> keysIter = serverSelector.selectedKeys().iterator();
                while (keysIter.hasNext())
                {
                    SelectionKey currentKey = null;
                    try
                    {
                        currentKey = keysIter.next();
                        keysIter.remove();

                        // Skip all operations if determining ready operations fails
                        int ops = 0;
                        ops = currentKey.readyOps();

                        if ((ops & OP_READ) != 0)
                        {
                            TcpConnectorPeer connPeer = null;
                            try
                            {
                                connPeer = (TcpConnectorPeer) currentKey.attachment();
                                ReadState state = connPeer.read((SocketChannel) currentKey.channel());
                                switch (state)
                                {
                                    case UNFINISHED:
                                        break;
                                    case FINISHED:
                                        msgProcessor.processMessage(connPeer.nextCurrentMsgIn(), this, connPeer);
                                        if (connPeer.hasNextMsgIn())
                                        {
                                            peersWithFinishedMessages.add(connPeer);
                                        }
                                        break;
                                    case END_OF_STREAM:
                                        final Node connNode = connPeer.getNode();
                                        if (connNode != null)
                                        {
                                            final NodeName name = connNode.getName();
                                            errorReporter.logInfo(
                                                "Remote satellite %s (peer %s) has closed the connection.",
                                                name.displayValue, connPeer.getId()
                                            );
                                        }
                                        closeConnection(currentKey, connPeer.isClientMode());
                                        break;
                                    default:
                                        throw new ImplementationError(
                                            String.format(
                                                "Missing case label for enum member '%s'",
                                                state.name()
                                            ),
                                            null
                                        );
                                }
                            }
                            catch (NotYetConnectedException connExc)
                            {
                                // This might possibly happen if an outbound connection is
                                // marked as READ interested before establishing the connection
                                // is finished; if the Selector would even report it as ready
                                // in this case.
                                // Anyway, the reason would be an implementation flaw of some
                                // kind, therefore, log this error and then treat the connection's
                                // state as a protocol error and close the connection.
                                errorReporter.reportError(new ImplementationError(connExc));
                                closeConnection(currentKey, true);
                            }
                            catch (IllegalMessageStateException msgStateExc)
                            {
                                errorReporter.reportError(
                                    new ImplementationError(
                                        "A message object with an illegal state was registered " +
                                        "as the target of an I/O read operation",
                                        msgStateExc
                                    )
                                );
                                closeConnection(currentKey, true);
                            }
                            catch (IOException ioExc)
                            {
                                // Protocol error - I/O error while reading a message
                                // Close the connection
                                errorReporter.reportError(
                                    Level.TRACE, ioExc, connPeer.getAccessContext(), connPeer,
                                    "I/O exception while attempting to receive data from the peer"
                                );
                                closeConnection(currentKey, true);
                            }
                        }
                        else
                        if ((ops & OP_ACCEPT) != 0)
                        {
                            try
                            {
                                acceptConnection(currentKey);
                            }
                            catch (ClosedChannelException closeExc)
                            {
                                // May be thrown by accept() if the server socket is closed
                                // Attempt to reinitialize to recover
                                reinitialize();
                                // Break out of iterating over keys, because those are all
                                // invalid after reinitialization, and the set of keys may have
                                // been modified too
                                break;
                            }
                            catch (NotYetBoundException unboundExc)
                            {
                                // Generated if accept() is invoked on an unbound server socket
                                // This should not happen, unless there is an
                                // implementation error somewhere.
                                // Attempt to reinitialize to recover
                                reinitialize();
                                // Break out of iterating over keys, because those are all
                                // invalid after reinitialization, and the set of keys may have
                                // been modified too
                                break;
                            }
                            catch (ClosedSelectorException closeExc)
                            {
                                // Throw by accept() if the selector is closed
                                // Attempt to reinitialize to recover
                                reinitialize();
                                // Break out of iterating over keys, because those are all
                                // invalid after reinitialization, and the set of keys may have
                                // been modified too
                                break;
                            }
                            catch (IOException ioExc)
                            {
                                errorReporter.reportError(
                                    Level.TRACE, ioExc, null, null,
                                    "I/O exception while attempting to accept a peer connection"
                                );
                            }
                        }
                        else
                        if ((ops & OP_WRITE) != 0)
                        {
                            TcpConnectorPeer connPeer = null;
                            try
                            {
                                connPeer = (TcpConnectorPeer) currentKey.attachment();
                                connPeer.write((SocketChannel) currentKey.channel());
                            }
                            catch (NotYetConnectedException connExc)
                            {
                                // This might possibly happen if an outbound connection is
                                // marked as WRITE interested before establishing the connection
                                // is finished; if the Selector would even report it as ready
                                // in this case.
                                // Anyway, the reason would be an implementation flaw of some
                                // kind, therefore, log this error and then treat the connection's
                                // state as a protocol error and close the connection.
                                errorReporter.reportError(new ImplementationError(connExc));
                                closeConnection(currentKey, true);
                            }
                            catch (IllegalMessageStateException msgStateExc)
                            {
                                errorReporter.reportError(
                                    new ImplementationError(
                                        "A message object with an illegal state was registered " +
                                        "as the target of an I/O write operation",
                                        msgStateExc
                                    )
                                );
                                closeConnection(currentKey, true);
                            }
                            catch (IOException ioExc)
                            {
                                // Protocol error - I/O error while writing a message
                                // Close channel / disconnect peer, invalidate SelectionKey
                                // Close the connection
                                errorReporter.reportError(
                                    Level.TRACE, ioExc, connPeer.getAccessContext(), connPeer,
                                    "I/O exception while attempting to send data to the peer"
                                );
                                closeConnection(currentKey, true);
                            }
                        }
                        else
                        if ((ops & OP_CONNECT) != 0)
                        {
                            TcpConnectorPeer connPeer = null;
                            try
                            {
                                connPeer = (TcpConnectorPeer) currentKey.attachment();
                                establishConnection(currentKey);
                            }
                            catch (IOException ioExc)
                            {
                                AccessContext peerAccCtx = null;
                                if (connPeer != null)
                                {
                                    peerAccCtx = connPeer.getAccessContext();
                                }
                                errorReporter.reportError(
                                    Level.TRACE, ioExc, peerAccCtx, connPeer,
                                    "I/O exception while attempting to connect to the peer"
                                );
                            }
                        }
                    }
                    catch (CancelledKeyException keyExc)
                    {
                        if (currentKey != null)
                        {
                            closeConnection(currentKey, true);
                        }
                    }
                    catch (IllegalStateException illState)
                    {
                        if (currentKey != null)
                        {
                            errorReporter.reportError(
                                new ImplementationError(
                                    "Unhandled IllegalStateException",
                                    illState
                                ),
                                null,
                                (Peer) currentKey.attachment(),
                                null
                            );
                            closeConnection(currentKey, true);
                        }
                    }
                }
            }
            catch (ClosedSelectorException selectExc)
            {
                // Selector became inoperative. Log error and attempt to reinitialize.
                errorReporter.reportError(selectExc);
                reinitialize();
            }
            catch (IOException ioExc)
            {
                // I/O error while selecting (likely), or an uncaught I/O error
                // while performing I/O on a channel (should not happen)
                // Log error and attempt to reinitialize.
                errorReporter.logDebug("IOException: %s", ioExc.getLocalizedMessage());
                errorReporter.reportError(Level.TRACE, ioExc);
                reinitialize();
            }
            catch (Exception exc)
            {
                // Uncaught exception. Log error and shut down.
                errorReporter.reportError(exc);
                break;
            }
            catch (ImplementationError implErr)
            {
                // Uncaught exception. Log error and shut down.
                errorReporter.reportError(implErr);
                break;
            }
        }
        uninitialize();

        synchronized (this)
        {
            selectorLoopThread = null;
        }
    }

    private void acceptConnection(SelectionKey currentKey)
        throws IOException
    {
        // Configure the socket for the accepted connection
        for (int loopCtr = 0; loopCtr < MAX_ACCEPT_LOOP; ++loopCtr)
        {
            SocketChannel newSocket = serverSocket.accept();
            if (newSocket != null)
            {
                boolean accepted = false;
                try
                {
                    newSocket.configureBlocking(false);
                    try
                    {
                        // IP_TOS can NOT be set on serverSocketChannel since SSC no longer supports that socketOption
                        // since java12. https://bugs.java.com/bugdatabase/view_bug?bug_id=8209152
                        newSocket.setOption(StandardSocketOptions.IP_TOS, IP_DSCP_OAM_LDP);
                        newSocket.socket().setTcpNoDelay(true);
                    }
                    catch (IOException ioExc)
                    {
                        // setting IP_TOS is just a "best effort" attempt.
                        errorReporter.logDebug("Failed to set IP_TOS to " + IP_DSCP_OAM_LDP);
                    }

                    // Generate the id for the peer object from the remote address of the connection
                    SocketAddress sockAddr = newSocket.getRemoteAddress();
                    try
                    {
                        InetSocketAddress inetSockAddr = (InetSocketAddress) sockAddr;
                        InetAddress inetAddr = inetSockAddr.getAddress();
                        if (inetAddr != null)
                        {
                            String peerId = inetAddr.getHostAddress() + ":" + inetSockAddr.getPort() + "/" +
                                connCount.incrementAndGet();

                            // Register the accepted connection with the selector loop
                            SelectionKey connKey = null;
                            try
                            {
                                connKey = newSocket.register(serverSelector, SelectionKey.OP_READ);
                            }
                            catch (IllegalSelectorException illSelExc)
                            {
                                // Thrown by register() if the selector is from another I/O provider
                                // than the channel that is being registered
                                errorReporter.reportError(
                                    new ImplementationError(
                                        "Registration of the channel with the selector failed, " +
                                        "because the channel was created by another type of " +
                                        "I/O provider",
                                        illSelExc
                                    )
                                );
                                // Connection was not accepted and will be closed in the finally block
                            }
                            catch (IllegalArgumentException illArg)
                            {
                                // Generated if a bit in the I/O operations specified
                                // in register() does not correspond with a supported I/O operation
                                // Should not happen; log the error.
                                // Connection was not accepted and will be closed in the finally block
                                errorReporter.reportError(illArg);
                            }

                            if (connKey != null)
                            {
                                // Prepare the peer object and message
                                TcpConnectorPeer connPeer = createTcpConnectorPeer(inetSockAddr, peerId, connKey, null);
                                connKey.attach(connPeer);
                                connPeer.connectionEstablished();
                                connObserver.inboundConnectionEstablished(connPeer);
                                accepted = true;
                            }
                        }
                        else
                        {
                            throw new IOException(
                                "Cannot generate the peer id, because the socket's " +
                                "internet address is uninitialized."
                            );
                        }
                    }
                    catch (ClassCastException ccExc)
                    {
                        throw new IOException(
                            "Peer connection address is not of type InetSocketAddress. " +
                            "Cannot generate the peer id."
                        );
                    }
                }
                catch (ClosedChannelException closeExc)
                {
                    // May be thrown by getRemoteAddress()
                    // Apparently, the peer closed the connection again before it could be accepted.
                    // No-op; connection was not accepted and will be closed in the finally block
                }
                catch (IllegalBlockingModeException illModeExc)
                {
                    // Implementation error, configureBlocking() skipped
                    errorReporter.reportError(
                        new ImplementationError(
                            "The accept() operation failed because the new socket channel's " +
                            "blocking mode was not configured correctly",
                            illModeExc
                        )
                    );
                    // Connection was not accepted and will be closed in the finally block
                }
                catch (CancelledKeyException cancelExc)
                {
                    // May be thrown by register() if the channel is already registered
                    // with the selector, but has already been cancelled too (which should
                    // be impossible, because the channel is first registered after being
                    // accepted)
                    errorReporter.reportError(cancelExc);
                    // Connection was not accepted and will be closed in the finally block
                }
                catch (IOException ioExc)
                {
                    // May be thrown by getRemoteAddress()
                    // This will cause the connection to be rejected because its endpoint
                    // address is undeterminable
                    errorReporter.reportError(ioExc);
                    // Connection was not accepted and will be closed in the finally block
                }
                finally
                {
                    if (!accepted)
                    {
                        // Close any rejected connections
                        try
                        {
                            newSocket.close();
                        }
                        catch (IOException ioExc)
                        {
                            errorReporter.reportError(ioExc);
                        }
                    }
                }
            }
            else
            {
                // No more connections to accept
                break;
            }
        }
    }

    protected TcpConnectorPeer createTcpConnectorPeer(
        InetSocketAddress peerHostAddr,
        String peerId,
        SelectionKey connKey,
        @Nullable Node node
    )
    {
        return createTcpConnectorPeer(peerHostAddr, peerId, connKey, false, node);
    }

    protected TcpConnectorPeer createTcpConnectorPeer(
        InetSocketAddress peerHostAddr,
        String peerId,
        SelectionKey connKey,
        boolean outgoing,
        @Nullable Node node
    )
    {
        return new TcpConnectorPeer(
            errorReporter, commonSerializer, peerHostAddr, peerId, this, connKey, defaultPeerAccCtx, node, outgoing
        );
    }

    @Override
    public void wakeup()
    {
        serverSelector.wakeup();
    }

    protected void establishConnection(SelectionKey currentKey)
        throws IOException
    {
        // first, remove OP_Connect interest (fixes an immediate return of selector.select()
        // with no ready keys bug)
        currentKey.interestOps(0); // when controller wants to send a message, this will be changed to
        // OP_WRITE automatically

        @SuppressWarnings("resource")
        SocketChannel channel = (SocketChannel) currentKey.channel();
        Peer peer = (Peer) currentKey.attachment();
        if (peer != null)
        {
            try
            {
                channel.finishConnect();
                peer.connectionEstablished();
                connObserver.outboundConnectionEstablished(peer);
            }
            catch (ConnectException conExc)
            {
                String message = conExc.getMessage();
                errorReporter.logTrace(
                    "Outbound connection to " + peer +
                    " failed" +
                    (message != null ? ": " + message : "")
                );
            }
            catch (NoRouteToHostException noRouteExc)
            {
                // ignore, Reconnector will retry later
            }
        }
        else
        {
            // The SocketChannel has no attached Peer object
            final SocketAddress localAddr = channel.getLocalAddress();
            final SocketAddress remoteAddr = channel.getRemoteAddress();

            String localAddrText = null;
            String remoteAddrText = null;

            try
            {
                final InetSocketAddress localInetAddr = (InetSocketAddress) localAddr;
                final InetAddress ipAddress = localInetAddr.getAddress();
                if (ipAddress != null)
                {
                    localAddrText = ipAddress.getHostAddress();
                }
            }
            catch (ClassCastException ignored)
            {
                // ignored
            }

            try
            {
                final InetSocketAddress remoteInetAddr = (InetSocketAddress) remoteAddr;
                final InetAddress ipAddress = remoteInetAddr.getAddress();
                if (ipAddress != null)
                {
                    remoteAddrText = ipAddress.getHostAddress();
                }
            }
            catch (ClassCastException ignored)
            {
                // ignored
            }

            final String errorDescription =
                this.getServiceName() + " connector: Connection " +
                (localAddrText != null ? "from " + localAddrText + " " : "") +
                (remoteAddrText != null ? "to " + remoteAddrText + " " : "") +
                "has no Peer object attached";

            errorReporter.reportError(
                Level.ERROR,
                new ImplementationError(errorDescription)
            );
        }
    }

    @Override
    public void closeConnection(TcpConnectorPeer peerObj, boolean allowReconnect)
    {
        closeConnection(peerObj.getSelectionKey(), allowReconnect);
    }

    private void closeConnection(SelectionKey currentKey, boolean allowReconnect)
    {
        closeConnection(currentKey, allowReconnect, false);
    }

    private void closeConnection(SelectionKey currentKey, boolean allowReconnect, boolean shuttingDown)
    {
        Peer client = (TcpConnectorPeer) currentKey.attachment();
        if (client != null)
        {
            try
            {
                if (client.isConnected(false))
                {
                    client.setAllowReconnect(allowReconnect);
                    client.connectionClosing();
                }
            }
            catch (CancelledKeyException ignored)
            {
                // connectionClosing() calls interestOps on the selection Key, which may fail
            }
            connObserver.connectionClosed(client, shuttingDown);
        }

        try
        {
            SelectableChannel channel = currentKey.channel();
            if (channel != null)
            {
                channel.close();
            }
        }
        catch (IOException closeIoExc)
        {
            // If close() fails with an I/O error, the reason may be interesting
            // enough to file an error report
            errorReporter.reportError(closeIoExc);
        }
        onConnectionClosed(currentKey);
        currentKey.cancel();
    }

    private void closeAllConnections()
    {
        try
        {
            if (serverSelector != null)
            {
                for (SelectionKey currentKey : serverSelector.keys())
                {
                    closeConnection(currentKey, false, true);
                }
                serverSelector.close();
            }
        }
        catch (ClosedSelectorException selectExc)
        {
            // Cannot close any connections, because the selector is inoperative
            errorReporter.reportError(selectExc);
        }
        catch (IOException ioExc)
        {
            // If close() fails with an I/O error, the reason may be interesting
            // enough to file an error report
            errorReporter.reportError(ioExc);
        }
    }

    private void closeServerSocket()
    {
        try
        {
            if (serverSocket != null)
            {
                serverSocket.close();
            }
        }
        catch (IOException ioExc)
        {
            // If close() fails with an I/O error, the reason may be interesting
            // enough to file an error report
            errorReporter.reportError(ioExc);
        }
    }

    public void initialize() throws IOException
    {
        serverSelector = Selector.open();

        if (bindAddress != null)
        {
            // setup bind socket
            boolean initFlag = false;
            try
            {
                serverSocket = ServerSocketChannel.open();
                IOException savedExc = null;
                try
                {
                    serverSocket.bind(bindAddress);
                }
                catch (AlreadyBoundException boundExc)
                {
                    // Thrown if this server socket is already bound.
                    // This is NOT the same error as if the TCP port is already in use,
                    // which will generate a java.net.BindException instead (subclass of IOException)
                    // This should not happen, unless there is a bug in the code that already
                    // bound the socket before trying to bind() again in this section of code
                    throw new ImplementationError(
                        "A newly created server socket could not be bound, because it is bound already.",
                        boundExc
                    );
                }
                catch (UnsupportedAddressTypeException addrExc)
                {
                    // Thrown if the socket can not be bound, because the type of
                    // address to bind the socket to is unsupported
                    savedExc = new IOException(
                        "Server socket creation failed, the specified server address " +
                            "is not of a supported type.",
                        addrExc
                    );
                }
                catch (ClosedChannelException closeExc)
                {
                    // Thrown if the socket is closed when bind() is called
                    savedExc = new IOException(
                        "Server socket creation failed. The server socket was closed " +
                            "while its initialization was still in progress.",
                        closeExc
                    );
                }
                catch (IOException ioExc)
                {
                    savedExc = ioExc;
                }

                // If binding the socket failed for a local or 'anylocal' IPv6 address,
                // fall back to the corresponding IPv4 addresses
                if (savedExc != null)
                {
                    InetSocketAddress inetBindAddress;
                    try
                    {
                        inetBindAddress = (InetSocketAddress) bindAddress;
                    }
                    catch (ClassCastException ccExc)
                    {
                        // bindAddress is not a known IP protocol
                        // No automatic failback is possible, throw the original exception
                        throw savedExc;
                    }

                    InetAddress addr = inetBindAddress.getAddress();
                    if (addr instanceof Inet6Address)
                    {
                        SocketAddress fallbackAddress = null;
                        if (addr.isLoopbackAddress())
                        {
                            errorReporter.logWarning(
                                "%s: Connector %s: Binding the socket to the IPv6 loopback address failed, " +
                                    "attempting fallback to IPv4",
                                SERVICE_NAME, serviceInstanceName
                            );
                            // IPv6 ::1 address, fallback to IPv4 127.0.0.1
                            fallbackAddress = new InetSocketAddress(
                                InetAddress.getByAddress(FALLBACK_LOOPBACK_ADDR),
                                inetBindAddress.getPort()
                            );
                        }
                        else
                        if (addr.isAnyLocalAddress())
                        {
                            errorReporter.logWarning(
                                "%s: Connector %s: Binding the socket to the IPv6 anylocal address failed, " +
                                    "attempting fallback to IPv4",
                                SERVICE_NAME, serviceInstanceName
                            );
                            // IPv6 ::0 address, fallback to IPv4 0.0.0.0
                            fallbackAddress = new InetSocketAddress(
                                InetAddress.getByAddress(FALLBACK_ANY_LOCAL_ADDR),
                                inetBindAddress.getPort()
                            );
                        }

                        if (fallbackAddress != null)
                        {
                            try
                            {
                                serverSocket.bind(fallbackAddress);
                                // Fallback succeeded, discard the exception
                                savedExc = null;
                            }
                            catch (AlreadyBoundException boundExc)
                            {
                                // Thrown if this server socket is already bound.
                                // This is NOT the same error as if the TCP port is already in use,
                                // see code comment further above in this same method for details.
                                throw new ImplementationError(
                                    "The IPv6 to IPv4 fallback code failed to bind the server socket, " +
                                        "because the socket is bound already.",
                                    boundExc
                                );
                            }
                            catch (UnsupportedAddressTypeException | IOException ignored)
                            {
                                // Fallback attempt failed
                                // Will throw the original exception further below
                                errorReporter.logError(
                                    "%s: Connector %s: Attempt to fallback to IPv4 failed",
                                    SERVICE_NAME, serviceInstanceName
                                );
                            }
                        }
                    }

                    // If there was no known IPv4 fallback method or if the fallback attempt failed,
                    // throw the original exception
                    if (savedExc != null)
                    {
                        throw savedExc;
                    }
                }

                serverSocket.configureBlocking(false);
                try
                {
                    serverSocket.register(serverSelector, OP_ACCEPT);
                }
                catch (IllegalBlockingModeException illModeExc)
                {
                    // Implementation error, configureBlocking() skipped
                    throw new ImplementationError(
                        "The server socket could not be initialized because its " +
                            "blocking mode was not configured correctly",
                        illModeExc
                    );
                }
                catch (ClosedSelectorException closeExc)
                {
                    // Thrown if the selector is closed when register() is called
                    throw new IOException(
                        "The server socket could not be initialized because the " +
                            "selector for the channel was closed when trying to register " +
                            "I/O operations for the server socket.",
                        closeExc
                    );
                }
                catch (CancelledKeyException cancelExc)
                {
                    // May be thrown by register() if the channel is already registered
                    // with the selector, but has already been cancelled too (which should
                    // be impossible, because the channel is first registered after being
                    // accepted)
                    throw new ImplementationError(
                        "Initialization of the server socket failed, because the socket's " +
                            "selection key was already registered and cancelled during initialization",
                        cancelExc
                    );
                }
                catch (IllegalSelectorException illSelExc)
                {
                    // Thrown by register() if the selector is from another I/O provider
                    // than the channel that is being registered
                    throw new ImplementationError(
                        "Initialization of the server socket failed because the channel " +
                            "was created by another type of I/O provider than the associated selector",
                        illSelExc
                    );
                }
                catch (IllegalArgumentException illArg)
                {
                    // Generated if a bit in the I/O operations specified
                    // in register() does not correspond with a supported I/O operation
                    // Should not happen; log the error.
                    // Server socket can not accept connections, treat this as an I/O error
                    throw new IOException(
                        "Configuring the server socket to accept new connections failed",
                        illArg
                    );
                }

                // Enable entering the run() method's selector loop
                shutdownFlag.set(false);
                initFlag = true;
            }
            finally
            {
                if (!initFlag)
                {
                    // Initialization failed, clean up
                    uninitialize();
                }
            }
        }
        else
        {
            shutdownFlag.set(false);
        }
    }

    private void uninitialize()
    {
        closeAllConnections();
        closeServerSocket();

        serverSocket    = null;
        serverSelector  = null;
    }

    /**
     * This method is synchronized because <code>this</code> should not be changed while we are re-initializing
     * Callers of this method will have to wait for a bit until reinitialization is finished
     */
    @SuppressFBWarnings("SWL_SLEEP_WITH_LOCK_HELD")
    private synchronized void reinitialize()
    {
        uninitialize();

        // Throttle reinitialization to avoid busy-looping in case of a
        // persistent error during initialization (e.g., all network drivers down, ...)
        try
        {
            Thread.sleep(REINIT_THROTTLE_TIME);
        }
        catch (InterruptedException intrExc)
        {
            // No-op; thread may be interrupted to shorten the sleep()
        }

        try
        {
            initialize();
        }
        catch (IOException ioExc)
        {
            errorReporter.reportError(ioExc);
        }
    }

    @Override
    public ServiceName getServiceName()
    {
        return SERVICE_NAME;
    }

    @Override
    public String getServiceInfo()
    {
        return SERVICE_INFO;
    }

    @Override
    public ServiceName getInstanceName()
    {
        return serviceInstanceName;
    }

    @Override
    public synchronized boolean isStarted()
    {
        return selectorLoopThread != null;
    }

    @Override
    public synchronized void setServiceInstanceName(ServiceName instanceName)
    {
        if (instanceName == null)
        {
            serviceInstanceName = SERVICE_NAME;
        }
        else
        {
            serviceInstanceName = instanceName;
        }
        if (selectorLoopThread != null)
        {
            selectorLoopThread.setName(serviceInstanceName.getDisplayName());
        }
    }

    protected void onSelectorWakeup()
        throws IllegalMessageStateException, IOException
    {
    }

    protected void onConnectionClosed(final SelectionKey currentKey)
    {
    }

    private static class SafeConnectionObserver implements ConnectionObserver
    {
        private final ErrorReporter errorReporter;
        private final ConnectionObserver connObserver;

        private SafeConnectionObserver(
            ErrorReporter errorReporterRef,
            ConnectionObserver connObserverRef
        )
        {
            errorReporter = errorReporterRef;
            connObserver = connObserverRef;
        }

        @Override
        public void outboundConnectionEstablished(Peer connPeer)
        {
            performConnectionObserverCall(
                notNullConnObserver -> notNullConnObserver.outboundConnectionEstablished(connPeer),
                "outbound connection established"
            );
        }

        @Override
        public void outboundConnectionEstablishing(Peer connPeer)
        {
            performConnectionObserverCall(
                notNullConnObserver -> notNullConnObserver.outboundConnectionEstablishing(connPeer),
                "outbound connection establishing"
            );
        }

        @Override
        public void inboundConnectionEstablished(Peer connPeer)
        {
            performConnectionObserverCall(
                notNullConnObserver -> notNullConnObserver.inboundConnectionEstablished(connPeer),
                "inbound connection established"
            );
        }

        @Override
        public void connectionClosed(Peer connPeer, boolean shuttingDown)
        {
            performConnectionObserverCall(
                notNullConnObserver -> notNullConnObserver.connectionClosed(connPeer, shuttingDown),
                "connection closed"
            );
        }

        private void performConnectionObserverCall(ConnectionObserverCall call, String eventDescription)
        {
            if (connObserver != null)
            {
                try
                {
                    call.run(connObserver);
                }
                catch (Exception exc)
                {
                    errorReporter.reportError(
                        exc,
                        null,
                        null,
                        "Exception thrown by connection observer when " + eventDescription
                    );
                }
            }
        }

        @FunctionalInterface
        private interface ConnectionObserverCall
        {
            void run(ConnectionObserver notNullConnObserver) throws Exception;
        }

    }
}
