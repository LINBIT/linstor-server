package com.linbit.linstor.core;

public interface DrbdStateChange
{
    default void drbdStateAvailable()
    {
        // Do nothing
    }

    default void drbdStateUnavailable()
    {
        // Do nothing
    }
}
