package com.linbit.linstor.event.handler;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.satellitestate.SatelliteState;

import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;

@Singleton
public class SatelliteStateHelper
{
    private final CoreModule.NodesMap nodesMap;
    private final ReadWriteLock nodesMapLock;

    @Inject
    public SatelliteStateHelper(
        CoreModule.NodesMap nodesMapRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef
    )
    {
        nodesMap = nodesMapRef;
        nodesMapLock = nodesMapLockRef;
    }

    public <T> @Nullable T withSatelliteState(
        NodeName nodeName,
        Function<SatelliteState, T> extractor,
        @Nullable T defaultIfNoPeer
    )
    {
        T value = defaultIfNoPeer;

        nodesMapLock.readLock().lock();
        try
        {
            Node node = nodesMap.get(nodeName);

            if (node != null)
            {
                Peer peer = node.getPeer();

                Lock writeLock = peer.getSatelliteStateLock().writeLock();
                writeLock.lock();
                try
                {
                    value = extractor.apply(peer.getSatelliteState());
                }
                finally
                {
                    writeLock.unlock();
                }
            }
        }
        finally
        {
            nodesMapLock.readLock().unlock();
        }

        return value;
    }

    public void onSatelliteState(NodeName nodeName, Consumer<SatelliteState> consumer)
    {
        withSatelliteState(
            nodeName,
            satelliteState -> acceptAndReturnNull(consumer, satelliteState),
            null
        );
    }

    private <T> @Nullable Void acceptAndReturnNull(Consumer<T> consumer, T value)
    {
        consumer.accept(value);
        return null;
    }
}
