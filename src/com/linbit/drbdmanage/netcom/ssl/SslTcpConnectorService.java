package com.linbit.drbdmanage.netcom.ssl;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.drbdmanage.CoreServices;
import com.linbit.drbdmanage.Node;
import com.linbit.drbdmanage.netcom.ConnectionObserver;
import com.linbit.drbdmanage.netcom.MessageProcessor;
import com.linbit.drbdmanage.netcom.TcpConnectorService;
import com.linbit.drbdmanage.security.AccessContext;

public class SslTcpConnectorService extends TcpConnectorService
{
    private final SSLContext sslCtx;

    public SslTcpConnectorService(
        final CoreServices coreSvcsRef,
        final MessageProcessor msgProcessorRef,
        final AccessContext peerAccCtxRef,
        final ConnectionObserver connObserverRef,
        final String sslProtocol,
        final String keyStoreFile,
        final char[] keyStorePasswd,
        final char[] keyPasswd,
        final String trustStoreFile,
        final char[] trustStorePasswd
    )
        throws IOException, NoSuchAlgorithmException, KeyManagementException,
        UnrecoverableKeyException, KeyStoreException, CertificateException
    {
        super(coreSvcsRef, msgProcessorRef, peerAccCtxRef, connObserverRef);
        sslCtx = SSLContext.getInstance(sslProtocol);
        initialize(keyStoreFile, keyStorePasswd, keyPasswd, trustStoreFile, trustStorePasswd);
    }

    public SslTcpConnectorService(
        final CoreServices coreSvcsRef,
        final MessageProcessor msgProcessorRef,
        final SocketAddress bindAddress,
        final AccessContext peerAccCtxRef,
        final ConnectionObserver connObserverRef,
        final String sslProtocol,
        final String keyStoreFile,
        final char[] keyStorePasswd,
        final char[] keyPasswd,
        final String trustStoreFile,
        final char[] trustStorePasswd
    )
        throws IOException, NoSuchAlgorithmException, KeyManagementException,
        UnrecoverableKeyException, KeyStoreException, CertificateException
    {
        super(coreSvcsRef, msgProcessorRef, bindAddress, peerAccCtxRef, connObserverRef);
        sslCtx = SSLContext.getInstance(sslProtocol);
        initialize(keyStoreFile, keyStorePasswd, keyPasswd, trustStoreFile, trustStorePasswd);
    }

    private void initialize(
        final String keyStoreFile,
        final char[] keyStorePasswd,
        final char[] keyPasswd,
        final String trustStoreFile,
        final char[] trustStorePasswd
    )
        throws ImplementationError, NoSuchAlgorithmException, KeyManagementException,
        KeyStoreException, IOException, CertificateException, UnrecoverableKeyException,
        FileNotFoundException
    {
        try
        {
            serviceInstanceName = new ServiceName("SSL"+serviceInstanceName.displayValue);
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

        sslCtx.init(
            SslTcpCommons.createKeyManagers(keyStoreFile, keyStorePasswd, keyPasswd),
            SslTcpCommons.createTrustManagers(trustStoreFile, trustStorePasswd),
            new SecureRandom()
        );
    }

    @Override
    protected SslTcpConnectorPeer createTcpConnectorPeer(
        final String peerId,
        final SelectionKey connKey,
        final boolean outgoing,
        final Node node
    )
    {
        try
        {
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
            }

            return new SslTcpConnectorPeer(
                peerId,
                this,
                connKey,
                defaultPeerAccCtx,
                sslCtx,
                address,
                node
            );
        }
        catch (SSLException sslExc)
        {
            throw new RuntimeException(sslExc);
        }
    }
}
