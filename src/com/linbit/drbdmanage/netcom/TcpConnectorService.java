package com.linbit.drbdmanage.netcom;

import com.linbit.*;
import com.linbit.drbdmanage.CoreServices;
import com.linbit.drbdmanage.TcpPortNumber;
import com.linbit.drbdmanage.netcom.TcpConnectorMessage.ReadState;
import com.linbit.drbdmanage.netcom.TcpConnectorMessage.WriteState;
import com.linbit.drbdmanage.security.AccessContext;

import java.io.IOException;
import java.net.*;
import java.nio.channels.*;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.nio.channels.SelectionKey.*;

/**
 * TCP/IP network communication service
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class TcpConnectorService implements Runnable, TcpConnector, SystemService
{
    private static final ServiceName SERVICE_NAME;
    private static final String SERVICE_INFO = "TCP/IP network communications service";

    private ServiceName serviceInstanceName;

    public static final int DEFAULT_PORT_VALUE = 9977;
    public static final TcpPortNumber DEFAULT_PORT;

    public static final InetAddress DEFAULT_BIND_INET_ADDRESS;
    public static final SocketAddress DEFAULT_BIND_ADDRESS;

    // Maximum number of connections to accept in one selector iteration
    public static final int MAX_ACCEPT_LOOP = 100;

    private CoreServices coreSvcs;
    private MessageProcessor msgProcessor;

    // Set by shutdown() to shut down the selector loop
    private AtomicBoolean shutdownFlag;

    // Selector loop thread
    private Thread selectorLoopThread;

    // Set to indicate that connections have been updated
    // outside of the selector loop
    private AtomicBoolean updateFlag;

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
            byte[] defaultIpv6Addr = new byte[16];
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
    };

    // Address that the server socket will be listening on
    private SocketAddress bindAddress;

    // Server socket for accepting incoming connections
    private ServerSocketChannel serverSocket;

    // Default access context for a newly connected peer
    private AccessContext defaultPeerAccCtx;

    // Selector for all connections
    Selector serverSelector;

    public TcpConnectorService(
        CoreServices coreSvcsRef,
        MessageProcessor msgProcessorRef,
        AccessContext defaultPeerAccCtxRef,
        ConnectionObserver connObserverRef
    ) throws IOException
    {
        ErrorCheck.ctorNotNull(TcpConnectorService.class, CoreServices.class, coreSvcsRef);
        ErrorCheck.ctorNotNull(TcpConnectorService.class, MessageProcessor.class, msgProcessorRef);
        ErrorCheck.ctorNotNull(TcpConnectorService.class, AccessContext.class, defaultPeerAccCtxRef);
        serviceInstanceName = SERVICE_NAME;

        bindAddress     = DEFAULT_BIND_ADDRESS;
        serverSocket    = null;
        serverSelector  = null;
        coreSvcs        = coreSvcsRef;
        msgProcessor    = msgProcessorRef;
        // Prevent entering the run() method's selector loop
        // until initialize() has completed
        shutdownFlag    = new AtomicBoolean(true);
        connObserver    = connObserverRef;

        defaultPeerAccCtx = defaultPeerAccCtxRef;
    }

    public TcpConnectorService(
        CoreServices coreSvcsRef,
        MessageProcessor msgProcessorRef,
        SocketAddress bindAddressRef,
        AccessContext defaultPeerAccCtxRef,
        ConnectionObserver connObserverRef
    ) throws IOException
    {
        this(coreSvcsRef, msgProcessorRef, defaultPeerAccCtxRef, connObserverRef);
        ErrorCheck.ctorNotNull(TcpConnectorService.class, SocketAddress.class, bindAddressRef);
        bindAddress = bindAddressRef;
    }

    public synchronized void initialize() throws SystemServiceStartException
    {
        try
        {
            if (shutdownFlag.get())
            {
                serverSocket = ServerSocketChannel.open();
                serverSelector = Selector.open();
                serverSocket.bind(bindAddress);
                serverSocket.configureBlocking(false);
                serverSocket.register(serverSelector, OP_ACCEPT);
                // Enable entering the run() method's selector loop
                shutdownFlag.set(false);
            }
        }
        catch (IOException ioExc)
        {
            throw new SystemServiceStartException(
                String.format(
                    "Initialization of the %s service instance '%s' failed.",
                    TcpConnectorService.class.getName(), serviceInstanceName
                ),
                ioExc
            );
        }
    }

    @Override
    public Peer connect()
    {
        // TODO: Implement connect() method
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public synchronized void start()
        throws SystemServiceStartException
    {
        if (selectorLoopThread == null)
        {
            initialize();
            selectorLoopThread = new Thread(this);
            selectorLoopThread.setName(serviceInstanceName.getDisplayName());
            selectorLoopThread.start();
        }
    }

    @Override
    public synchronized void shutdown()
    {
        shutdownFlag.set(true);
        if (serverSelector != null)
        {
            serverSelector.wakeup();
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
        while (!shutdownFlag.get())
        {
            try
            {
                // Block until I/O operations are ready to be performed
                // on at least one of the channels, or until the selection
                // operation is interrupted (e.g., using wakeup())
                int selectCount = serverSelector.select();

                // Ensure making some progress in the case that
                // the blocking select() call is repeatedly interrupted
                // (e.g., using wakeup()) before having selected any
                // channels
                if (selectCount <= 0)
                {
                    serverSelector.selectNow();
                }

                Iterator<SelectionKey> keysIter = serverSelector.selectedKeys().iterator();
                while (keysIter.hasNext())
                {
                    SelectionKey currentKey = keysIter.next();
                    keysIter.remove();
                    int ops = currentKey.readyOps();
                    if ((ops & OP_READ) != 0)
                    {
                        try
                        {
                            TcpConnectorPeer connPeer = (TcpConnectorPeer) currentKey.attachment();
                            ReadState state = connPeer.msgIn.read((SocketChannel) currentKey.channel());
                            switch (state)
                            {
                                case UNFINISHED:
                                    break;
                                case FINISHED:
                                    msgProcessor.processMessage(connPeer.msgIn, this, connPeer);
                                    connPeer.nextInMessage();
                                    break;
                                case END_OF_STREAM:
                                    closeConnection(currentKey);
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
                        catch (IllegalMessageStateException msgStateExc)
                        {
                            // FIXME: Implementation error
                            coreSvcs.getErrorReporter().reportError(msgStateExc);
                            closeConnection(currentKey);
                        }
                        catch (IOException ioExc)
                        {
                            // Protocol error:
                            // I/O error while reading a message
                            // Close channel / disconnect peer, invalidate SelectionKey
                            coreSvcs.getErrorReporter().reportError(ioExc);
                            closeConnection(currentKey);
                        }
                    }
                    else
                    if ((ops & OP_WRITE) != 0)
                    {
                        try
                        {
                            TcpConnectorPeer connPeer = (TcpConnectorPeer) currentKey.attachment();
                            WriteState state = connPeer.msgOut.write((SocketChannel) currentKey.channel());
                            switch (state)
                            {
                                case UNFINISHED:
                                    break;
                                case FINISHED:
                                    connPeer.nextOutMessage();
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
                        catch (IllegalMessageStateException msgStateExc)
                        {
                            // FIXME: Implementation error
                            coreSvcs.getErrorReporter().reportError(msgStateExc);
                            closeConnection(currentKey);
                        }
                        catch (IOException ioExc)
                        {
                            // Protocol error:
                            // I/O error while writing a message
                            // Close channel / disconnect peer, invalidate SelectionKey
                            coreSvcs.getErrorReporter().reportError(ioExc);
                            closeConnection(currentKey);
                        }
                    }
                    else
                    if ((ops & OP_ACCEPT) != 0)
                    {
                        try
                        {
                            acceptConnection(currentKey);
                        }
                        catch (IOException ioExc)
                        {
                            coreSvcs.getErrorReporter().reportError(ioExc);
                        }
                    }
                    else
                    if ((ops & OP_CONNECT) != 0)
                    {
                        try
                        {
                            establishConnection(currentKey);
                        }
                        catch (IOException ioExc)
                        {
                            coreSvcs.getErrorReporter().reportError(ioExc);
                        }
                    }
                }
            }
            catch (ClosedSelectorException selectExc)
            {
                // FIXME
                // Selector became inoperative.
                // A possible recovery would be to reinitialize the TcpConnectorService
                coreSvcs.getErrorReporter().reportError(selectExc);
            }
            catch (IOException ioExc)
            {
                // FIXME
                // I/O error while selecting, or an uncaught I/O error while performing
                // I/O on a single channel (this should not happen)
                // A possible recovery would be to reinitialize the TcpConnectorService
                coreSvcs.getErrorReporter().reportError(ioExc);
            }
            catch (Exception exc)
            {
                coreSvcs.getErrorReporter().reportError(exc);
            }
            catch (ImplementationError implErr)
            {
                coreSvcs.getErrorReporter().reportError(implErr);
            }
        }

        try
        {
            if (serverSelector != null)
            {
                for (SelectionKey currentKey : serverSelector.keys())
                {
                    closeConnection(currentKey);
                }
            }
            serverSelector.close();
        }
        catch (ClosedSelectorException selectExc)
        {
            // FIXME
            coreSvcs.getErrorReporter().reportError(selectExc);
        }
        catch (ClosedChannelException closedExc)
        {
            // FIXME
            coreSvcs.getErrorReporter().reportError(closedExc);
        }
        catch (IOException ioExc)
        {
            // FIXME
            coreSvcs.getErrorReporter().reportError(ioExc);
        }

        try
        {
            serverSocket.close();
        }
        catch (IOException ioExc)
        {
            // FIXME
            coreSvcs.getErrorReporter().reportError(ioExc);
        }
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

                    // Generate the id for the peer object from the remote address of the connection
                    SocketAddress sockAddr = newSocket.getRemoteAddress();
                    try
                    {
                        InetSocketAddress inetSockAddr = (InetSocketAddress) sockAddr;
                        InetAddress inetAddr = inetSockAddr.getAddress();
                        if (inetAddr != null)
                        {
                            String peerId = inetAddr.getHostAddress() + ":" + inetSockAddr.getPort();

                            // Register the accepted connection with the selector loop
                            SelectionKey connKey = newSocket.register(serverSelector, SelectionKey.OP_READ);

                            // Prepare the peer object and message
                            TcpConnectorPeer connPeer = new TcpConnectorPeer(
                                peerId, this, connKey, defaultPeerAccCtx
                            );
                            connKey.attach(connPeer);
                            if (connObserver != null)
                            {
                                connObserver.inboundConnectionEstablished(connPeer);
                            }
                            accepted = true;
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
                            coreSvcs.getErrorReporter().reportError(ioExc);
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

    @Override
    public void wakeup()
    {
        serverSelector.wakeup();
    }

    private void establishConnection(SelectionKey currentKey)
        throws IOException
    {
        // TODO
    }

    private void closeConnection(SelectionKey currentKey)
    {
        Peer client = (TcpConnectorPeer) currentKey.attachment();
        if (connObserver != null && client != null)
        {
            connObserver.connectionClosed(client);
        }
        try
        {
            SelectableChannel channel = currentKey.channel();
            if (channel != null)
            {
                currentKey.channel().close();
            }
        }
        catch (IOException closeIoExc)
        {
            // FIXME
            coreSvcs.getErrorReporter().reportError(closeIoExc);
        }
        catch (IllegalStateException illState)
        {
            // No-op; may be thrown when the connection has been closed
        }
        currentKey.cancel();
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
        selectorLoopThread.setName(serviceInstanceName.getDisplayName());
    }
}
