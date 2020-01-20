package com.linbit.linstor.core.cfg;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;

public class CtrlConfigModule extends AbstractModule
{
    private final CtrlConfig ctrlConfig;

    public CtrlConfigModule(CtrlConfig ctrlConfigRef)
    {
        ctrlConfig = ctrlConfigRef;
    }

    @Provides
    CtrlConfig getCtrlConfig()
    {
        return ctrlConfig;
    }
}
