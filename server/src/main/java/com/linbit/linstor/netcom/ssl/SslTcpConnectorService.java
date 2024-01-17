package com.linbit.linstor.netcom.ssl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;

import javax.net.ssl.SSLContext;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.modularcrypto.ModularCryptoProvider;
import com.linbit.linstor.netcom.ConnectionObserver;
import com.linbit.linstor.netcom.IllegalMessageStateException;
import com.linbit.linstor.netcom.MessageProcessor;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.netcom.TcpConnectorService;
import com.linbit.linstor.security.AccessContext;
import java.util.Map.Entry;
import java.util.TreeMap;
import javax.annotation.Nonnull;


public class SslTcpConnectorService extends TcpConnectorService
{
    // Begin debug logging flags

    /**
     * If enabled, generates INFO level log entries for the connector's initialization
     */
    private final boolean           DEBUG_INIT          = false;

    /**
     * If enabled, generates INFO level log entries for new connections and when connections are closed
     */
    private final boolean           DEBUG_CONNECTION    = false;

    /**
     * If enabled, generates INFO level log entries for tracking the state of concurrently executing
     * SSLEngine delegated tasks
     */
    private final boolean           DEBUG_SSL_TASKS     = false;

    // End debug logging flags


    private final ModularCryptoProvider cryptoProvider;
    private final SSLContext sslCtx;

    private final TreeMap<String, SslTcpConnectorPeer>  taskCompletionMap;

    /**
     * Constructs a new instance of the SSL/TLS netcom connector
     *
     * @param errorReporter ErrorReporter to be used for logging
     * @param commonSerializer Serializer for various protobuf messages
     * @param msgProcessorRef Message processor that should be invoked for processing inbound messages
     * @param bindAddress Local bind address for the connector
     * @param peerAccCtxRef Default access context for unauthenticated peers
     * @param privAccCtxRef Access context for executing privileged operations
     * @param connObserverRef Connection observer to notify when peers connect or disconnect
     * @param cryptoProviderRef Provider for the implementation of cryptographic algorithms
     * @param sslProtocol The SSL protocol to use
     * @param keyStoreFile Path of the file containing the cryptographic keys
     * @param keyStorePasswd Password for decrypting key store file
     * @param keyPasswd Password for decrypting the keys contained in the key store file
     * @param trustStoreFile Path of the trust store file, which contains the trusted SSL certificates
     *                       (e.g., CA certificates)
     * @param trustStorePasswd Password for decrypting the trust store file
     * @throws IOException Indicates a problem with accessing the key store file or trust store file, such as
     *                     absence of the file, no permission to access the file or failed I/O requests
     * @throws NoSuchAlgorithmException If a required cryptographic algorithm is not available on the system
     * @throws KeyManagementException Indicates a problem with cryptographic keys, such as conflicting/duplicate IDs,
     *                                expired keys, authorization failure, etc.
     * @throws UnrecoverableKeyException Indicates that a key cannot be recovered from the key store file
     * @throws KeyStoreException Indicates a problem with processing the key store file
     * @throws CertificateException Indicates a problem with an SSL certificate, such as an encoding problem,
     *                              use outside of the period of validity, a certificate being revoked, etc.
     */
    public SslTcpConnectorService(
        @Nonnull final ErrorReporter            errorReporter,
        @Nonnull final CommonSerializer         commonSerializer,
        @Nonnull final MessageProcessor         msgProcessorRef,
        @Nonnull final SocketAddress            bindAddress,
        @Nonnull final AccessContext            peerAccCtxRef,
        @Nonnull final AccessContext            privAccCtxRef,
        @Nonnull final ConnectionObserver       connObserverRef,
        @Nonnull final ModularCryptoProvider    cryptoProviderRef,
        @Nonnull final String                   sslProtocol,
        @Nonnull final String                   keyStoreFile,
        @Nonnull final char[]                   keyStorePasswd,
        @Nonnull final char[]                   keyPasswd,
        @Nonnull final String                   trustStoreFile,
        @Nonnull final char[]                   trustStorePasswd
    )
        throws IOException, NoSuchAlgorithmException, KeyManagementException,
        UnrecoverableKeyException, KeyStoreException, CertificateException
    {
        super(
            errorReporter,
            commonSerializer,
            msgProcessorRef,
            bindAddress,
            peerAccCtxRef,
            privAccCtxRef,
            connObserverRef
        );
        if (DEBUG_INIT)
        {
            final String debugSslProtocol = (sslProtocol != null ? "\"" + sslProtocol + "\"" : "null");
            final String debugKeyStoreFile = (keyStoreFile != null ? "\"" + keyStoreFile + "\"" : "null");
            final String debugKeyStorePassword = (keyStorePasswd != null ? "\"" + keyStorePasswd + "\"" : "null");
            final String debugKeyPassword = (keyPasswd != null ? "\"" + keyPasswd + "\"" : "null");
            final String debugTrustStoreFile = (trustStoreFile != null ? "\"" + trustStoreFile + "\"" : "null");
            final String debugTrustStorePassword = (trustStorePasswd != null ? "\"" + trustStorePasswd + "\"" : "null");

            debugLog(
                "Constructor: " +
                "sslProtocol=" + debugSslProtocol +
                ", keyStoreFile=" + debugKeyStoreFile +
                ", keyStorePassword=" + debugKeyStorePassword +
                ", keyPassword=" + debugKeyPassword +
                ", trustStoreFile=" + debugTrustStoreFile +
                ", trustStorePassword=" + debugTrustStorePassword
            );
        }
        taskCompletionMap = new TreeMap<>();
        cryptoProvider = cryptoProviderRef;
        if (DEBUG_INIT)
        {
            debugLog("Constructor: Creating SSL context");
        }
        sslCtx = cryptoProviderRef.createSslContext(sslProtocol);
        initialize(keyStoreFile, keyStorePasswd, keyPasswd, trustStoreFile, trustStorePasswd);
        if (DEBUG_INIT)
        {
            debugLog("Constructor: Connector construction complete");
        }
    }

    /**
     * Initializes the SSLContext for this SSL connection
     *
     * @param keyStoreFile Path of the file containing the cryptographic keys
     * @param keyStorePasswd Password for decrypting key store file
     * @param keyPasswd Password for decrypting the keys contained in the key store file
     * @param trustStoreFile Path of the trust store file, which contains the trusted SSL certificates
     *                       (e.g., CA certificates)
     * @param trustStorePasswd Password for decrypting the trust store file
     * @throws NoSuchAlgorithmException If a required cryptographic algorithm is not available on the system
     * @throws KeyManagementException Indicates a problem with cryptographic keys, such as conflicting/duplicate IDs,
     *                                expired keys, authorization failure, etc.
     * @throws KeyStoreException Indicates a problem with processing the key store file
     * @throws IOException Indicates a problem with accessing the key store file or trust store file, such as
     *                     absence of the file, no permission to access the file or failed I/O requests
     * @throws CertificateException Indicates a problem with an SSL certificate, such as an encoding problem,
     *                              use outside of the period of validity, a certificate being revoked, etc.
     * @throws UnrecoverableKeyException Indicates that a key cannot be recovered from the key store file
     */
    private void initialize(
        @Nonnull final String   keyStoreFile,
        @Nonnull final char[]   keyStorePasswd,
        @Nonnull final char[]   keyPasswd,
        @Nonnull final String   trustStoreFile,
        @Nonnull final char[]   trustStorePasswd
    )
        throws NoSuchAlgorithmException, KeyManagementException,
        KeyStoreException, IOException, CertificateException, UnrecoverableKeyException
    {
        try
        {
            serviceInstanceName = new ServiceName("SSL" + serviceInstanceName.displayValue);
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

        if (DEBUG_INIT)
        {
            debugLog("initialize: Initializing SSL context");
        }
        cryptoProvider.initializeSslContext(
            sslCtx,
            keyStoreFile, keyStorePasswd, keyPasswd,
            trustStoreFile, trustStorePasswd
        );
        if (DEBUG_INIT)
        {
            debugLog("initialize: Initialization complete");
        }
    }

    /**
     * Creates a new SslTcpConnectorPeer instance.
     * Called when an outbound connection attempt is made or an inbound connection is accepted by the netcom connector.
     *
     * @param peerId LINSTOR netcom ID for this connection (usually identified by the peer's address)
     * @param connKey SelectionKey associated with this connection, for the nonblocking I/O Selector
     * @param outgoing true if called while making an outbound connection attempt;
     *                 false if accepting an inbound connection
     * @param node LINSTOR Node object associated with this connection (e.g., the connected LINSTOR satellite)
     * @return SslTcpConnectorPeer instance representing the connection to the peer
     */
    @Override
    protected SslTcpConnectorPeer createTcpConnectorPeer(
        @Nonnull final String       peerId,
        @Nonnull final SelectionKey connKey,
        final boolean               outgoing,
        @Nonnull final Node         node
    )
    {
        if (DEBUG_CONNECTION)
        {
            String nodeName = null;
            if (node != null)
            {
                nodeName = node.getName().displayValue;
            }
            debugLog(
                "createTcpConnectorPeer: New " + (outgoing ? "outbound" : "inbound") + " connection, " +
                "peerId=" + (peerId != null ? "\"" + peerId + "\"" : "null") +
                (nodeName != null ? ", node name=\"" + nodeName + "\"" : "")
            );
        }
        InetSocketAddress address = null;
        if (outgoing)
        {
            @SuppressWarnings("resource")
            SocketChannel channel = (SocketChannel) connKey.channel();
            @SuppressWarnings("resource")
            Socket socket = channel.socket();
            String host = socket.getInetAddress().getHostAddress();
            int port = socket.getPort();
            address = new InetSocketAddress(host, port);
            if (DEBUG_CONNECTION)
            {
                debugLog("createTcpConnectorPeer: Outbound connection address=" + host + ":" + port);
            }
        }

        final SslTcpConnectorPeer newPeer = new SslTcpConnectorPeer(
            errorReporter,
            commonSerializer,
            peerId,
            this,
            connKey,
            defaultPeerAccCtx,
            sslCtx,
            address,
            node
        );
        return newPeer;
    }

    /**
     * Called when the last concurrently executing SSLEngine task pending for a peer completes.
     * Wakes up the Selector, which then invokes the peer's SSL handshake handling code to continue the SSL handshake
     * and re-enable I/O operations for the peer.
     *
     * @param connPeer The peer object associated with the task completion
     */
    protected void taskCompleted(@Nonnull final SslTcpConnectorPeer connPeer)
    {
        if (DEBUG_SSL_TASKS)
        {
            debugLog("taskCompleted: Connector notified of delegated SSL tasks completion");
        }
        synchronized (taskCompletionMap)
        {
            final String id = connPeer.getId();
            if (id != null)
            {
                if (DEBUG_SSL_TASKS)
                {
                    debugLog("taskCompleted: Peer added to taskCompletionMap, peerId=\"" + id + "\"");
                }
                taskCompletionMap.put(id, connPeer);
            }
        }
        wakeup();
    }

    /**
     * Called when the nonblocking I/O Selector returns from selecting operations.
     * As of Nov 21, 2023, handles continuation of the SSL handshake after I/O operations had been suspended to
     * wait for the completion of SSLEngine tasks that are executed concurrently in separate threads
     *
     * @throws IllegalMessageStateException If the LINSTOR message is in an illegal state for the data transfer
     *         operation performed by SSL processing methods; not supposed to happen, indicates an implementation error
     * @throws IOException If network I/O operations fail while continuing the SSL handshake
     */
    @Override
    protected void onSelectorWakeup()
        throws IllegalMessageStateException, IOException
    {
        if (DEBUG_SSL_TASKS)
        {
            debugLog("onSelectorWakeup called");
        }
        for (SslTcpConnectorPeer connPeer = nextTaskCompletionEntry();
             connPeer != null;
             connPeer = nextTaskCompletionEntry())
        {
            if (DEBUG_SSL_TASKS)
            {
                final String peerId = connPeer.getId();
                debugLog(
                    "onSelectorWakeup: Calling sslTasksCompleted, peerId=" +
                    (peerId != null ? "\"" + peerId + "\"" : "null")
                );
            }
            connPeer.sslTasksCompleted();
        }
    }

    /**
     * Called when the netcom connector closes a connection.
     * As of Nov 21, 2023, this cancels continuation of an SSL handshake upon completion of SSLEngine tasks that
     * are executed concurrently in separate threads. This handles the case where a network connection is already
     * closed when the SSLEngine tasks are completed, and therefore, continuing the handshake no longer makes sense.
     *
     * @param currentKey SelectionKey associated with the connection to the peer
     */
    @Override
    protected void onConnectionClosed(@Nonnull final SelectionKey currentKey)
    {
        final Peer connPeer = (Peer) currentKey.attachment();
        if (connPeer instanceof SslTcpConnectorPeer)
        {
            final SslTcpConnectorPeer sslPeer = (SslTcpConnectorPeer) connPeer;
            if (DEBUG_CONNECTION)
            {
                final String peerId = connPeer.getId();
                debugLog(
                    "onConnectionClosed: Connection closed, peerId=" +
                    (peerId != null ? "\"" + peerId + "\"" : "null") +
                    ", calling cancelSslTasks"
                );
            }
            sslPeer.cancelSslTasks();
        }
    }

    /**
     * Returns the next peer that is ready for continuing the SSL handshake.
     * Used to iterate through the peers that were scheduled for continuing the SSL handshake after completion of
     * concurrently running SSLEngine tasks.
     *
     * @return SslTcpConnectorPeer object scheduled for continuing the SSL handshake
     */
    private SslTcpConnectorPeer nextTaskCompletionEntry()
    {
        SslTcpConnectorPeer connPeer;
        synchronized (taskCompletionMap)
        {
            final Entry<String, SslTcpConnectorPeer> entry = taskCompletionMap.pollFirstEntry();
            connPeer = entry == null ? null : entry.getValue();
        }
        return connPeer;
    }

    private void debugLog(final String logMsg)
    {
        errorReporter.logInfo("%s", getClass().getName() + ": DEBUG: " + logMsg);
    }
}
