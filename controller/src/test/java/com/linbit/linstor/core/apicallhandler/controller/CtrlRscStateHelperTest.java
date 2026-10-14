package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.satellitestate.SatelliteResourceState;

import java.util.Map;
import java.util.TreeMap;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CtrlRscStateHelperTest
{
    private static SatelliteResourceState state(@Nullable Boolean inUse, @Nullable Boolean open)
    {
        SatelliteResourceState state = new SatelliteResourceState();
        state.setInUse(inUse);
        state.setOpen(open);
        return state;
    }

    private static boolean hasPrimaryOrOpen(SatelliteResourceState... states) throws Exception
    {
        Map<NodeName, SatelliteResourceState> stateMap = new TreeMap<>();
        for (int idx = 0; idx < states.length; idx++)
        {
            stateMap.put(new NodeName("node" + idx), states[idx]);
        }
        return CtrlRscStateHelper.hasPrimaryOrOpen(stateMap);
    }

    @Test
    public void primary() throws Exception
    {
        assertThat(hasPrimaryOrOpen(state(false, false), state(true, true))).isTrue();
    }

    @Test
    public void openOnASecondary() throws Exception
    {
        assertThat(hasPrimaryOrOpen(state(false, false), state(false, true))).isTrue();
    }

    @Test
    public void closedSecondaries() throws Exception
    {
        assertThat(hasPrimaryOrOpen(state(false, false), state(false, false))).isFalse();
    }

    @Test
    public void unknownStates() throws Exception
    {
        // satellites or DRBD versions that do not report the open state fall back to the role
        assertThat(hasPrimaryOrOpen(state(false, null), state(null, null))).isFalse();
        assertThat(CtrlRscStateHelper.hasPrimaryOrOpen(null)).isFalse();
    }
}
