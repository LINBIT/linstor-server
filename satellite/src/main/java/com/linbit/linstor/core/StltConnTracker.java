package com.linbit.linstor.core;

import com.linbit.linstor.SatellitePeerCtx;
import com.linbit.linstor.event.EventBroker;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.ConnectionObserver;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.storage.StorageException;
import com.linbit.utils.ExceptionThrowingRunnable;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Objects;

@Singleton
public class StltConnTracker implements ConnectionObserver
{
    private final CoreModule.PeerMap peerMap;
    private final EventBroker eventBroker;
    private final ControllerPeerConnector controllerPeerConnector;
    private final Provider<DeviceManager> devMgrProvider;
    private final ArrayList<ExceptionThrowingRunnable<StorageException>> closingListeners = new ArrayList<>();
    private final ErrorReporter errorReporter;

    @Inject
    StltConnTracker(
        CoreModule.PeerMap peerMapRef,
        EventBroker eventBrokerRef,
        ControllerPeerConnector controllerPeerConnectorRef,
        Provider<DeviceManager> devMgrProviderRef,
        ErrorReporter errorReporterRef
    )
    {
        peerMap = peerMapRef;
        eventBroker = eventBrokerRef;
        controllerPeerConnector = controllerPeerConnectorRef;
        devMgrProvider = devMgrProviderRef;
        errorReporter = errorReporterRef;
    }

    @Override
    public void outboundConnectionEstablished(Peer connPeer)
    {
        // FIXME: Something should done here for completeness, although the Satellite
        //        does not normally connect outbound
        addToPeerMap(connPeer);
    }

    @Override
    public void outboundConnectionEstablishing(Peer peerRef) throws IOException
    {
        addToPeerMap(peerRef);
    }

    private void addToPeerMap(Peer connPeer)
    {
        if (connPeer != null)
        {
            SatellitePeerCtx peerCtx = (SatellitePeerCtx) connPeer.getAttachment();
            if (peerCtx == null)
            {
                peerCtx = new SatellitePeerCtx();
                connPeer.attach(peerCtx);
            }
            synchronized (peerMap)
            {
                peerMap.put(connPeer.getId(), connPeer);
            }
        }
    }

    @Override
    public void inboundConnectionEstablished(Peer connPeer)
    {
        if (connPeer != null)
        {
            SatellitePeerCtx peerCtx = new SatellitePeerCtx();
            connPeer.attach(peerCtx);
            synchronized (peerMap)
            {
                peerMap.put(connPeer.getId(), connPeer);
            }
        }
    }

    @Override
    public void connectionClosed(Peer connPeer, boolean allowReconnect, boolean shuttingDown)
    {
        if (connPeer != null)
        {
            if (Objects.equals(connPeer, controllerPeerConnector.getControllerPeer()))
            {
                devMgrProvider.get().controllerConnectionLost();
            }
            eventBroker.connectionClosed(connPeer);

            synchronized (peerMap)
            {
                peerMap.remove(connPeer.getId());
            }
            synchronized (closingListeners)
            {
                for (ExceptionThrowingRunnable<StorageException> closingListener : closingListeners)
                {
                    try
                    {
                        closingListener.run();
                    }
                    catch (StorageException exc)
                    {
                        errorReporter.reportError(exc);
                    }
                }
            }
        }
    }

    public void addClosingListener(ExceptionThrowingRunnable<StorageException> run)
    {
        synchronized (closingListeners)
        {
            closingListeners.add(run);
        }
    }
}
