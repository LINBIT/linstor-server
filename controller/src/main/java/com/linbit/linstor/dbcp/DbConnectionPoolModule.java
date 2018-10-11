package com.linbit.linstor.dbcp;

import com.linbit.linstor.DatabaseInfo;
import com.linbit.linstor.DatabaseInfoImpl;
import com.google.inject.AbstractModule;
import com.linbit.linstor.ControllerDatabase;

public class DbConnectionPoolModule extends AbstractModule
{
    @Override
    protected void configure()
    {
        bind(ControllerDatabase.class).to(DbConnectionPool.class);
        bind(DatabaseInfo.class).to(DatabaseInfoImpl.class);
    }
}
