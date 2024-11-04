package com.linbit.linstor.event;

import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;

import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import reactor.core.Disposable;


public class WatchStoreTest
{
    private static final String TEST_EVENT_NAME = "TestEventName";

    private NodeName testNodeName;
    private ResourceName testResourceName;
    private VolumeNumber testVolumeNumber;

    private EventIdentifier globalEventIdentifier;
    private EventIdentifier resourceEventIdentifier;
    private EventIdentifier volumeEventIdentifier;

    private WatchStore watchStore;

    @Before
    public void setUp()
        throws Exception
    {
        testNodeName = new NodeName("TestNodeName");
        testResourceName = new ResourceName("TestResourceName");
        testVolumeNumber = new VolumeNumber(4);

        globalEventIdentifier = EventIdentifier.global(TEST_EVENT_NAME);
        resourceEventIdentifier = EventIdentifier.resource(TEST_EVENT_NAME, testNodeName, testResourceName);
        volumeEventIdentifier =
            EventIdentifier.volume(TEST_EVENT_NAME, testNodeName, testResourceName, testVolumeNumber);

        watchStore = new WatchStoreImpl();
    }

    @Test
    public void removeWatchesForPeer()
        throws Exception
    {
        Disposable disposable = Mockito.mock(Disposable.class);
        Disposable otherPeerDisposable = Mockito.mock(Disposable.class);

        String testPeerId = "TestPeer";

        Watch peerWatch =
            new Watch(UUID.randomUUID(), testPeerId, 0, resourceEventIdentifier);
        Watch otherPeerWatch =
            new Watch(UUID.randomUUID(), "OtherPeer", 0, resourceEventIdentifier);
        watchStore.addWatch(peerWatch, disposable);
        watchStore.addWatch(otherPeerWatch, otherPeerDisposable);

        watchStore.removeWatchesForPeer(testPeerId);

        Mockito.verify(disposable).dispose();
        Mockito.verify(otherPeerDisposable, Mockito.never()).dispose();
    }

    @Test
    public void removeWatchForPeerAndId()
        throws Exception
    {
        Disposable disposable = Mockito.mock(Disposable.class);
        Disposable otherDisposable = Mockito.mock(Disposable.class);
        Disposable otherPeerDisposable = Mockito.mock(Disposable.class);

        String testPeerId = "TestPeer";
        Integer testWatchId = 7;

        Watch peerWatch =
            new Watch(UUID.randomUUID(), testPeerId, testWatchId, resourceEventIdentifier);
        Watch otherWatch =
            new Watch(UUID.randomUUID(), testPeerId, 8, resourceEventIdentifier);
        Watch otherPeerWatch =
            new Watch(UUID.randomUUID(), "OtherPeer", 9, resourceEventIdentifier);
        watchStore.addWatch(peerWatch, disposable);
        watchStore.addWatch(otherWatch, otherDisposable);
        watchStore.addWatch(otherPeerWatch, otherPeerDisposable);

        watchStore.removeWatchForPeerAndId(testPeerId, testWatchId);

        Mockito.verify(disposable).dispose();
        Mockito.verify(otherDisposable, Mockito.never()).dispose();
        Mockito.verify(otherPeerDisposable, Mockito.never()).dispose();
    }
}
