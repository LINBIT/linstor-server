package com.linbit.linstor.drbdstate;

import com.google.inject.AbstractModule;

public class DrbdStateModule extends AbstractModule
{
    @Override
    protected void configure()
    {
        bind(DrbdStateTracker.class).to(DrbdEventService.class);
    }
}
