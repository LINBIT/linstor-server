package com.linbit.drbdmanage.netcom.ssl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
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
import com.linbit.drbdmanage.netcom.ConnectionObserver;
import com.linbit.drbdmanage.netcom.MessageProcessor;
import com.linbit.drbdmanage.netcom.TcpConnectorService;
import com.linbit.drbdmanage.security.AccessContext;

public class SslTcpConnectorService extends TcpConnectorService
{
    private SSLContext sslCtx;

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
        try
        {
            serviceInstanceName = new ServiceName("SSL"+serviceInstanceName.value);
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

        sslCtx = SSLContext.getInstance(sslProtocol);
        sslCtx.init(
            SslTcpCommons.createKeyManagers(keyStoreFile, keyStorePasswd, keyPasswd),
            SslTcpCommons.createTrustManagers(trustStoreFile, trustStorePasswd),
            new SecureRandom()
        );
    }

    @Override
    protected void establishConnection(final SelectionKey key) throws IOException
    {
        super.establishConnection(key);
        // this method should only be called for outgoing connections
        // thus, we have to be currently in client mode
        key.interestOps(SelectionKey.OP_WRITE);
//        SocketChannel channel = (SocketChannel) key.channel();
//        SslTcpConnectorPeer peer = (SslTcpConnectorPeer) key.attachment();
//        peer.encryptConnection(channel);
    }

    @Override
    protected SslTcpConnectorPeer createTcpConnectorPeer(
        final String peerId,
        final SelectionKey connKey,
        final boolean outgoing
    )
    {
        try
        {
            InetSocketAddress address = null;
            if(outgoing)
            {
                SocketChannel channel = (SocketChannel) connKey.channel();
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
                address);
        }
        catch (SSLException sslExc)
        {
            throw new RuntimeException(sslExc);
        }
    }
}
