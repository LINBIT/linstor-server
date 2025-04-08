package com.linbit.linstor.netcom;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.prop.Property;
import com.linbit.linstor.core.cfg.StltConfig;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.satellitestate.SatelliteState;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.utils.externaltools.ExtToolsManager;

import javax.annotation.Nullable;
import javax.net.ssl.SSLException;

import java.io.ByteArrayInputStream;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

public class PeerOffline implements Peer
{
    private final String peerId;
    private final Node node;
    static ServiceName serviceName;

    private final ErrorReporter errorReporter;
    private final ReadWriteLock satelliteStateLock;
    private SatelliteState satelliteState;
    private final ExtToolsManager extToolMgr;

    static
    {
        try
        {
            serviceName = new ServiceName("PeerOffline");
        }
        catch (InvalidNameException ignored)
        {
        }
    }

    public PeerOffline(
        ErrorReporter errorReporterRef,
        String peerIdRef,
        Node nodeRef
    )
    {
        errorReporter = errorReporterRef;
        peerId = peerIdRef;
        node = nodeRef;
        satelliteStateLock = new ReentrantReadWriteLock(true);
        extToolMgr = new ExtToolsManager();

        satelliteState = new SatelliteState();
    }

    @Override
    public String getId()
    {
        return peerId;
    }

    @Nullable
    @Override
    public InetSocketAddress getHostAddr()
    {
        return null;
    }

    @Override
    public Node getNode()
    {
        return node;
    }

    @Override
    public ServiceName getConnectorInstanceName()
    {
        return serviceName;
    }

    @Override
    public AccessContext getAccessContext()
    {
        return null;
    }

    @Override
    public void setAccessContext(AccessContext privilegedCtx, AccessContext newAccCtx) throws AccessDeniedException
    {
    }

    @Override
    public void attach(Object attachment)
    {
    }

    @Override
    public Object getAttachment()
    {
        return null;
    }

    @Override
    public Message createMessage()
    {
        return null;
    }

    @Override
    public boolean sendMessage(Message msg) throws IllegalMessageStateException
    {
        errorReporter.logTrace("Skipping message since this is an instance of PeerOffline");
        return false;
    }

    @Override
    public boolean sendMessage(byte[] data)
    {
        return false;
    }

    @Override
    public long getNextIncomingMessageSeq()
    {
        return 0;
    }

    @Override
    public void processInOrder(long peerSeq, Publisher<?> publisher)
    {
    }

    @Override
    public Flux<ByteArrayInputStream> apiCall(String apiCallName, byte[] data)
    {
        return Flux.empty();
    }

    @Override
    public void apiCallAnswer(long apiCallId, ByteArrayInputStream data)
    {
    }

    @Override
    public void apiCallError(long apiCallId, Throwable exc)
    {
    }

    @Override
    public void apiCallComplete(long apiCallId)
    {
    }

    @Override
    public void setAllowReconnect(boolean ignoredAllowReconnectRef)
    {
    }

    @Override
    public boolean isAllowReconnect()
    {
        return false;
    }

    @Override
    public void closeConnection()
    {
    }

    @Override
    public void closeConnection(boolean allowReconnect)
    {
    }

    @Override
    public void connectionClosing()
    {
    }

    @Override
    public boolean isOnline()
    {
        return false;
    }

    @Override
    public ApiConsts.ConnectionStatus getConnectionStatus()
    {
        return ApiConsts.ConnectionStatus.OFFLINE;
    }

    @Override
    public void setConnectionStatus(ApiConsts.ConnectionStatus status)
    {
    }

    @Override
    public boolean isConnected(boolean ensureAuthenticated)
    {
        return false;
    }

    @Override
    public boolean isAuthenticated()
    {
        return false;
    }

    @Override
    public void setAuthenticated(boolean authenticated)
    {
    }

    @Override
    public int outQueueCapacity()
    {
        return 0;
    }

    @Override
    public int outQueueCount()
    {
        return 0;
    }

    @Override
    public long msgSentCount()
    {
        return 0;
    }

    @Override
    public long msgRecvCount()
    {
        return 0;
    }

    @Override
    public long msgSentMaxSize()
    {
        return 0;
    }

    @Override
    public long msgRecvMaxSize()
    {
        return 0;
    }

    @Override
    public InetSocketAddress peerAddress()
    {
        return null;
    }

    @Override
    public InetSocketAddress localAddress()
    {
        return null;
    }

    @Override
    public void connectionEstablished() throws SSLException
    {
    }

    @Override
    public void waitUntilConnectionEstablished() throws InterruptedException
    {
    }

    @Override
    public TcpConnector getConnector()
    {
        return null;
    }

    @Override
    public void sendPing()
    {
    }

    @Override
    public void sendPong()
    {
    }

    @Override
    public void pongReceived()
    {
    }

    @Override
    public long getLastPingSent()
    {
        return 0;
    }

    @Override
    public long getLastPongReceived()
    {
        return 0;
    }

    @Override
    public ReadWriteLock getSatelliteStateLock()
    {
        return satelliteStateLock;
    }

    @Override
    public SatelliteState getSatelliteState()
    {
        return satelliteState;
    }

    @Override
    public ReadWriteLock getSerializerLock()
    {
        return null;
    }

    @Override
    public void setFullSyncId(long timestamp)
    {
    }

    @Override
    public long getFullSyncId()
    {
        return 0;
    }

    @Override
    public long getNextSerializerId()
    {
        return 0;
    }

    @Override
    public void fullSyncFailed(ApiConsts.ConnectionStatus ignored)
    {
    }

    @Override
    public boolean hasFullSyncFailed()
    {
        return false;
    }

    @Override
    public void fullSyncApplied()
    {
        throw new ImplementationError("FullSync cannot have been applied to an Offline Peer");
    }

    @Override
    public boolean isFullSyncApplied()
    {
        return false;
    }

    @Override
    public boolean hasNextMsgIn()
    {
        return false;
    }

    @Override
    public Message nextCurrentMsgIn()
    {
        return null;
    }

    @Override
    public ExtToolsManager getExtToolsManager()
    {
        return extToolMgr;
    }

    @Override
    public StltConfig getStltConfig()
    {
        return null;
    }

    @Override
    public void setStltConfig(StltConfig stltConfig)
    {
    }

    @Override
    public void setDynamicProperties(List<Property> dynamicPropListRef)
    {
        // no-op
    }

    @Override
    public Property getDynamicProperty(String keyRef)
    {
        return null;
    }
}
