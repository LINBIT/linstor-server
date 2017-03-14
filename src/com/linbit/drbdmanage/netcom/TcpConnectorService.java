package com.linbit.drbdmanage.netcom;

import com.linbit.ErrorCheck;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbdmanage.Controller;
import com.linbit.drbdmanage.CoreServices;
import com.linbit.drbdmanage.TcpPortNumber;
import com.linbit.drbdmanage.netcom.TcpConnectorMessage.ReadState;
import com.linbit.drbdmanage.netcom.TcpConnectorMessage.WriteState;
import com.linbit.drbdmanage.security.AccessContext;
import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import static java.nio.channels.SelectionKey.OP_ACCEPT;
import static java.nio.channels.SelectionKey.OP_CONNECT;
import static java.nio.channels.SelectionKey.OP_READ;
import static java.nio.channels.SelectionKey.OP_WRITE;

/**
 * TCP/IP network communication service
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class TcpConnectorService extends Thread implements TcpConnector, SystemService
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

    // Set to indicate that connections have been updated
    // outside of the selector loop
    private AtomicBoolean updateFlag;

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
        AccessContext defaultPeerAccCtxRef
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

        defaultPeerAccCtx = defaultPeerAccCtxRef;
    }

    public TcpConnectorService(
        CoreServices coreSvcsRef,
        MessageProcessor msgProcessorRef,
        SocketAddress bindAddressRef,
        AccessContext defaultPeerAccCtxRef
    ) throws IOException
    {
        this(coreSvcsRef, msgProcessorRef, defaultPeerAccCtxRef);
        ErrorCheck.ctorNotNull(TcpConnectorService.class, SocketAddress.class, bindAddressRef);
        bindAddress = bindAddressRef;
    }

    public void initialize() throws IOException, ClosedChannelException
    {
        serverSocket = ServerSocketChannel.open();
        serverSelector = Selector.open();
        serverSocket.bind(bindAddress);
        serverSocket.configureBlocking(false);
        serverSocket.register(serverSelector, OP_ACCEPT);
        // Enable entering the run() method's selector loop
        shutdownFlag.set(false);
    }

    @Override
    public Peer connect()
    {
        // TODO: Implement connect() method
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void shutdown()
    {
        shutdownFlag.set(true);
        serverSelector.wakeup();
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
                        acceptConnection(currentKey);
                    }
                    else
                    if ((ops & OP_CONNECT) != 0)
                    {
                        establishConnection(currentKey);
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
            for (SelectionKey currentKey : serverSelector.keys())
            {
                closeConnection(currentKey);
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
    }

    private void acceptConnection(SelectionKey currentKey)
        throws IOException
    {
        Controller.logInfo("Accepting new connections"); // DEBUG
        // Configure the socket for the accepted connection
        for (int loopCtr = 0; loopCtr < MAX_ACCEPT_LOOP; ++loopCtr)
        {
            SocketChannel newSocket = serverSocket.accept();
            if (newSocket != null)
            {
                newSocket.configureBlocking(false);

                // Register the accepted connection with the selector loop
                SelectionKey connKey = newSocket.register(serverSelector, SelectionKey.OP_READ);

                // Prepare the peer object and message
                TcpConnectorPeer connPeer = new TcpConnectorPeer(this, currentKey, defaultPeerAccCtx);
                connKey.attach(connPeer);

                // BEGIN DEBUG
                String addrStr = "<unknown>";
                try
                {
                    SocketChannel sChannel = (SocketChannel) connKey.channel();
                    if (sChannel != null)
                    {
                        SocketAddress sAddr = sChannel.getRemoteAddress();
                        if (sAddr != null)
                        {
                            addrStr = sAddr.toString();
                        }
                    }

                }
                catch (Exception exc)
                {
                    coreSvcs.getErrorReporter().reportError(exc);
                }
                Controller.logInfo(
                    String.format(
                        "Accepted connection to %s",
                        addrStr
                    )
                );
                // END DEBUG
            }
            else
            {
                Controller.logInfo("No more connections to accept at this time"); // DEBUG
                break;
            }
        }
    }

    void enableSend(SelectionKey currentKey)
    {

    }

    private void establishConnection(SelectionKey currentKey)
    {
        // TODO
    }

    private void closeConnection(SelectionKey currentKey)
    {
        // BEGIN DEBUG
        String addrStr = "<unknown>";
        try
        {
            SelectableChannel selChannel = currentKey.channel();
            if (selChannel instanceof SocketChannel)
            {
                SocketChannel sChannel = (SocketChannel) selChannel;
                if (sChannel != null)
                {
                    SocketAddress sAddr = sChannel.getRemoteAddress();
                    if (sAddr != null)
                    {
                        addrStr = sAddr.toString();
                    }
                }
            }
        }
        catch (Exception exc)
        {
            coreSvcs.getErrorReporter().reportError(exc);
        }
        Controller.logInfo(
            String.format(
                "Closing connection to %s",
                addrStr
            )
        );
        // END DEBUG
        try
        {
            currentKey.channel().close();
        }
        catch (IOException closeIoExc)
        {
            // FIXME
            coreSvcs.getErrorReporter().reportError(closeIoExc);
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
        return this.isAlive();
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
        setName(serviceInstanceName.getDisplayName());
    }
}
