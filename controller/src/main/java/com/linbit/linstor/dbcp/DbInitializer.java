package com.linbit.linstor.dbcp;

import com.linbit.SystemServiceStartException;
import com.linbit.linstor.InitializationException;
import com.linbit.linstor.systemstarter.StartupInitializer;

public interface DbInitializer extends StartupInitializer
{
    @Override
    void initialize()
        throws InitializationException, SystemServiceStartException;
}
