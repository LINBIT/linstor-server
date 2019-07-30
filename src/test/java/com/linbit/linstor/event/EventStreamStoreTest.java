package com.linbit.linstor.event;

import org.junit.Before;
import org.junit.Test;

import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;

import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;

public class EventStreamStoreTest
{
    private static final String TEST_EVENT_NAME = "TestEventName";

    private NodeName testNodeName;
    private ResourceName testResourceName;
    private VolumeNumber testVolumeNumber;

    private EventIdentifier globalEventIdentifier;
    private EventIdentifier resourceEventIdentifier;
    private EventIdentifier volumeDefinitionEventIdentifier;
    private EventIdentifier volumeEventIdentifier;

    private EventStreamStore eventStreamStore;

    @Before
    public void setUp()
        throws Exception
    {
        testNodeName = new NodeName("TestNodeName");
        testResourceName = new ResourceName("TestResourceName");
        testVolumeNumber = new VolumeNumber(4);

        globalEventIdentifier = EventIdentifier.global(TEST_EVENT_NAME);
        resourceEventIdentifier = EventIdentifier.resource(TEST_EVENT_NAME, testNodeName, testResourceName);
        volumeDefinitionEventIdentifier =
            EventIdentifier.volumeDefinition(TEST_EVENT_NAME, testResourceName, testVolumeNumber);
        volumeEventIdentifier =
            EventIdentifier.volume(TEST_EVENT_NAME, testNodeName, testResourceName, testVolumeNumber);

        eventStreamStore = new EventStreamStoreImpl();
    }

    @Test
    public void getStreamsForSameEvent()
        throws Exception
    {
        eventStreamStore.addEventStream(volumeEventIdentifier);

        Collection<EventIdentifier> eventStreams = eventStreamStore.getDescendantEventStreams(volumeEventIdentifier);

        assertThat(eventStreams).containsExactly(volumeEventIdentifier);
    }

    @Test
    public void getStreamsForParentEvent()
        throws Exception
    {
        eventStreamStore.addEventStream(volumeEventIdentifier);

        Collection<EventIdentifier> eventStreams = eventStreamStore.getDescendantEventStreams(resourceEventIdentifier);

        assertThat(eventStreams).containsExactly(volumeEventIdentifier);
    }

    @Test
    public void getStreamsForAncestorEvent()
        throws Exception
    {
        eventStreamStore.addEventStream(volumeEventIdentifier);

        Collection<EventIdentifier> eventStreams = eventStreamStore.getDescendantEventStreams(globalEventIdentifier);

        assertThat(eventStreams).containsExactly(volumeEventIdentifier);
    }

    @Test
    public void noStreamsForUnrelatedEvent()
        throws Exception
    {
        eventStreamStore.addEventStream(resourceEventIdentifier);

        Collection<EventIdentifier> eventStreams =
            eventStreamStore.getDescendantEventStreams(volumeDefinitionEventIdentifier);

        assertThat(eventStreams).isEmpty();
    }

    @Test
    public void removeStream()
        throws Exception
    {
        eventStreamStore.addEventStream(volumeEventIdentifier);
        Collection<EventIdentifier> eventStreams =
            eventStreamStore.getDescendantEventStreams(volumeDefinitionEventIdentifier);
        assertThat(eventStreams).containsExactly(volumeEventIdentifier);

        eventStreamStore.removeEventStream(volumeEventIdentifier);
        Collection<EventIdentifier> eventStreamsAfterRemove =
            eventStreamStore.getDescendantEventStreams(volumeDefinitionEventIdentifier);
        assertThat(eventStreamsAfterRemove).isEmpty();
    }
}
