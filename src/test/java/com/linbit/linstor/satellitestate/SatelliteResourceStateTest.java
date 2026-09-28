package com.linbit.linstor.satellitestate;

import com.linbit.linstor.annotation.Nullable;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SatelliteResourceStateTest
{
    private static @Nullable Boolean inUseOrOpen(@Nullable Boolean inUse, @Nullable Boolean open)
    {
        SatelliteResourceState state = new SatelliteResourceState();
        state.setInUse(inUse);
        state.setOpen(open);
        return state.isInUseOrOpen();
    }

    @Test
    public void openSecondaryIsInUseOrOpen()
    {
        assertThat(inUseOrOpen(false, true)).isTrue();
        assertThat(inUseOrOpen(null, true)).isTrue();
    }

    @Test
    public void primaryIsInUseOrOpen()
    {
        assertThat(inUseOrOpen(true, false)).isTrue();
        assertThat(inUseOrOpen(true, null)).isTrue();
    }

    @Test
    public void unknownOpenFallsBackToInUse()
    {
        assertThat(inUseOrOpen(false, null)).isFalse();
        assertThat(inUseOrOpen(null, null)).isNull();
    }

    @Test
    public void closedSecondaryIsNotInUseOrOpen()
    {
        assertThat(inUseOrOpen(false, false)).isFalse();
    }

    @Test
    public void copyKeepsOpen()
    {
        SatelliteResourceState state = new SatelliteResourceState();
        state.setOpen(true);
        assertThat(new SatelliteResourceState(state).isOpen()).isTrue();
    }
}
