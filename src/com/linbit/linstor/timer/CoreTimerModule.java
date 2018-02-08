package com.linbit.linstor.timer;

import com.google.inject.AbstractModule;

public class CoreTimerModule extends AbstractModule
{
    @Override
    protected void configure()
    {
        bind(CoreTimer.class).to(CoreTimerImpl.class);
    }
}
