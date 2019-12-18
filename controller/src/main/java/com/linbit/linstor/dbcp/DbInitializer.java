package com.linbit.linstor.dbcp;

import com.linbit.linstor.InitializationException;

public interface DbInitializer
{
    void initialize(boolean withStartupVer)
        throws InitializationException;
}
