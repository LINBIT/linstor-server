package com.linbit.linstor.drbdstate;

import com.google.inject.AbstractModule;

public class DrbdStateModule extends AbstractModule
{
    @Override
    protected void configure()
    {
        bind(DrbdStateStore.class).to(DrbdEventService.class);
    }
}
