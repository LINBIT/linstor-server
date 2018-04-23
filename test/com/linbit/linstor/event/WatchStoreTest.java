package com.linbit.linstor.event;

import com.linbit.linstor.NodeName;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.VolumeNumber;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

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

        globalEventIdentifier = new EventIdentifier(
            TEST_EVENT_NAME, null, null, null);
        resourceEventIdentifier = new EventIdentifier(
            TEST_EVENT_NAME, testNodeName, testResourceName, null);
        volumeEventIdentifier = new EventIdentifier(
            TEST_EVENT_NAME, testNodeName, testResourceName, testVolumeNumber);

        watchStore = new WatchStoreImpl();
    }

    @Test
    public void getWatchesForPreciseEvent()
        throws Exception
    {
        Watch watch = makeWatch(volumeEventIdentifier);
        watchStore.addWatch(watch);

        Collection<Watch> watches = watchStore.getWatchesForEvent(volumeEventIdentifier);

        assertThat(watches).containsExactly(watch);
    }

    @Test
    public void getWatchesGlobalForVolumeEvent()
        throws Exception
    {
        Watch watch = makeWatch(globalEventIdentifier);
        watchStore.addWatch(watch);

        Collection<Watch> watches = watchStore.getWatchesForEvent(volumeEventIdentifier);

        assertThat(watches).containsExactly(watch);
    }

    @Test
    public void noWatchesResourceForGlobalEvent()
        throws Exception
    {
        Watch watch = makeWatch(resourceEventIdentifier);
        watchStore.addWatch(watch);

        Collection<Watch> watches = watchStore.getWatchesForEvent(globalEventIdentifier);

        assertThat(watches).isEmpty();
    }

    @Test
    public void getWatchesResourceOnlyForResourceEvent()
        throws Exception
    {
        Watch resourceWatch = makeWatch(resourceEventIdentifier);
        Watch volumeWatch = makeWatch(volumeEventIdentifier);
        watchStore.addWatch(resourceWatch);
        watchStore.addWatch(volumeWatch);

        Collection<Watch> watches = watchStore.getWatchesForEvent(resourceEventIdentifier);

        assertThat(watches).containsExactly(resourceWatch);
    }

    @Test
    public void getWatchesResourceAndVolumeForVolumeEvent()
        throws Exception
    {
        Watch resourceWatch = makeWatch(resourceEventIdentifier);
        Watch volumeWatch = makeWatch(volumeEventIdentifier);
        watchStore.addWatch(resourceWatch);
        watchStore.addWatch(volumeWatch);

        Collection<Watch> watches = watchStore.getWatchesForEvent(volumeEventIdentifier);

        assertThat(watches).containsExactlyInAnyOrder(resourceWatch, volumeWatch);
    }

    @Test
    public void removeWatchesForPeer()
        throws Exception
    {
        String testPeerId = "TestPeer";

        Watch peerWatch =
            new Watch(UUID.randomUUID(), testPeerId, 0, resourceEventIdentifier);
        Watch otherPeerWatch =
            new Watch(UUID.randomUUID(), "OtherPeer", 0, resourceEventIdentifier);
        watchStore.addWatch(peerWatch);
        watchStore.addWatch(otherPeerWatch);

        watchStore.removeWatchesForPeer(testPeerId);

        assertThat(watchStore.getWatchesForEvent(resourceEventIdentifier))
            .containsExactly(otherPeerWatch);
    }

    @Test
    public void removeWatchForPeerAndId()
        throws Exception
    {
        String testPeerId = "TestPeer";
        Integer testWatchId = 7;

        Watch peerWatch =
            new Watch(UUID.randomUUID(), testPeerId, testWatchId, resourceEventIdentifier);
        Watch otherWatch =
            new Watch(UUID.randomUUID(), testPeerId, 8, resourceEventIdentifier);
        Watch otherPeerWatch =
            new Watch(UUID.randomUUID(), "OtherPeer", 9, resourceEventIdentifier);
        watchStore.addWatch(peerWatch);
        watchStore.addWatch(otherWatch);
        watchStore.addWatch(otherPeerWatch);

        watchStore.removeWatchForPeerAndId(testPeerId, testWatchId);

        assertThat(watchStore.getWatchesForEvent(resourceEventIdentifier))
            .containsExactlyInAnyOrder(otherWatch, otherPeerWatch);
    }

    @Test
    public void removeWatchesForObject()
        throws Exception
    {
        Watch globalWatch = makeWatch(globalEventIdentifier);
        Watch resourceWatch = makeWatch(resourceEventIdentifier);
        Watch volumeWatch = makeWatch(volumeEventIdentifier);
        watchStore.addWatch(globalWatch);
        watchStore.addWatch(resourceWatch);
        watchStore.addWatch(volumeWatch);

        watchStore.removeWatchesForObject(
            new ObjectIdentifier(testNodeName, testResourceName, null)
        );

        assertThat(watchStore.getWatchesForEvent(volumeEventIdentifier))
            .containsExactlyInAnyOrder(globalWatch, volumeWatch);
    }

    private Watch makeWatch(EventIdentifier resourceEventIdentifier)
    {
        return new Watch(UUID.randomUUID(), null, 0, resourceEventIdentifier);
    }
}
