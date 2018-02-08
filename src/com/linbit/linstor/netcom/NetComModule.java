package com.linbit.linstor.netcom;

import com.google.inject.AbstractModule;

public class NetComModule extends AbstractModule
{
    @Override
    protected void configure()
    {
        bind(NetComContainer.class).to(NetComContainerImpl.class);
    }
}
