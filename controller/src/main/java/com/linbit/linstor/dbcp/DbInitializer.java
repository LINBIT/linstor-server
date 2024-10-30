package com.linbit.linstor.dbcp;

import com.linbit.SystemServiceStartException;
import com.linbit.linstor.InitializationException;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.systemstarter.StartupInitializer;

public interface DbInitializer extends StartupInitializer
{
    void setEnableMigrationOnInit(boolean enableRef);

    boolean needsMigration() throws DatabaseException, InitializationException;

    void migrateTo(Object version) throws DatabaseException, InitializationException;

    @Override
    void initialize() throws InitializationException, SystemServiceStartException;
}
