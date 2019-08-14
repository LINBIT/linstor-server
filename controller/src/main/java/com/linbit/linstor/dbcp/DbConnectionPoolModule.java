package com.linbit.linstor.dbcp;

import com.linbit.linstor.ControllerDatabase;

import com.google.inject.AbstractModule;

public class DbConnectionPoolModule extends AbstractModule
{
    @Override
    protected void configure()
    {
        bind(ControllerDatabase.class).to(DbConnectionPool.class);
    }
}
