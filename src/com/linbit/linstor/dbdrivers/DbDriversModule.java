package com.linbit.linstor.dbdrivers;

import com.google.inject.AbstractModule;

public class DbDriversModule extends AbstractModule
{
    @Override
    protected void configure()
    {
        bind(DatabaseDriver.class).to(DerbyDriver.class);
    }
}
