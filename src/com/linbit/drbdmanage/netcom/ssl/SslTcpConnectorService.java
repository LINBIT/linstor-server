package com.linbit.drbdmanage.netcom.ssl;

import java.io.IOException;
import java.net.InetSocketAddress;
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
import com.linbit.drbdmanage.netcom.Peer;
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
    public Peer connect(final InetSocketAddress address)
    {
        // TODO: Implement SSLConnectorService.connect()
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    protected void establishConnection(final SelectionKey key) throws IOException
    {
        SocketChannel channel = (SocketChannel) key.channel();
        SslTcpConnectorPeer peer = (SslTcpConnectorPeer) key.attachment();
        peer.encryptConnection(channel);
    }

    @Override
    protected SslTcpConnectorPeer createTcpConnectorPeer(
        final String peerId,
        final SelectionKey connKey
    )
    {
        try
        {
            return new SslTcpConnectorPeer(peerId, this, connKey, defaultPeerAccCtx, sslCtx, null);
        }
        catch (SSLException sslExc)
        {
            throw new RuntimeException(sslExc);
        }
    }
}
