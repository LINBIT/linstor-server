package com.linbit.linstor.layer.storage.utils;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.stateflags.StateFlags;

import java.util.Set;

import org.junit.Test;
import org.mockito.Answers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SharedStorageUtilsTest
{
    private static final String NODE_NAME = "node-b";

    @Test
    public void activeLegHoldsLock() throws Exception
    {
        assertThat(SharedStorageUtils.holdsOrAwaitsLock(mockLeg(Set.of(), null))).isTrue();
    }

    @Test
    public void deactivatingLegHoldsLock() throws Exception
    {
        Resource leg = mockLeg(Set.of(Resource.Flags.INACTIVE, Resource.Flags.INACTIVATING), null);
        assertThat(SharedStorageUtils.holdsOrAwaitsLock(leg)).isTrue();
    }

    @Test
    public void inactiveLeftoverDoesNotHoldLock() throws Exception
    {
        assertThat(SharedStorageUtils.holdsOrAwaitsLock(mockLeg(Set.of(Resource.Flags.INACTIVE), null))).isFalse();
    }

    @Test
    public void inactiveLiveMigrationTargetAwaitsLock() throws Exception
    {
        Resource leg = mockLeg(Set.of(Resource.Flags.INACTIVE), NODE_NAME.toUpperCase());
        assertThat(SharedStorageUtils.holdsOrAwaitsLock(leg)).isTrue();
    }

    @Test
    public void inactiveLegOfOtherLiveMigrationTargetDoesNotHoldLock() throws Exception
    {
        Resource leg = mockLeg(Set.of(Resource.Flags.INACTIVE), "node-c");
        assertThat(SharedStorageUtils.holdsOrAwaitsLock(leg)).isFalse();
    }

    @SuppressWarnings("unchecked")
    private static Resource mockLeg(Set<Resource.Flags> setFlags, @Nullable String sharedTargetMarker)
        throws Exception
    {
        StateFlags<Resource.Flags> flags = mock(StateFlags.class);
        when(flags.isSet(any(Resource.Flags.class)))
            .thenAnswer(invocation -> setFlags.contains(invocation.getArgument(0, Resource.Flags.class)));

        Resource rsc = mock(Resource.class, Answers.RETURNS_DEEP_STUBS);
        when(rsc.getStateFlags()).thenReturn(flags);
        when(rsc.getNode().getName()).thenReturn(new NodeName(NODE_NAME));
        when(
            rsc.getResourceDefinition().getProps().getProp(
                InternalApiConsts.KEY_LIVE_MIGRATE_SHARED_TARGET_NODE,
                InternalApiConsts.NAMESPC_LIVE_MIGRATE
            )
        ).thenReturn(sharedTargetMarker);
        return rsc;
    }
}
