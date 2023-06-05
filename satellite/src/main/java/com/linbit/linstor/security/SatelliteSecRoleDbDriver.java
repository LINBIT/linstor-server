package com.linbit.linstor.security;

import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.SatelliteSingleColDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecRoleDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SatelliteSecRoleDbDriver implements SecRoleDatabaseDriver
{
    private final SingleColumnDatabaseDriver<Role, SecurityType> noopDomainDriver = new SatelliteSingleColDriver<>();
    private final SingleColumnDatabaseDriver<Role, Boolean> noopRoleEnabled = new SatelliteSingleColDriver<>();

    @Inject
    public SatelliteSecRoleDbDriver()
    {
    }

    @Override
    public void create(Role dataRef) throws DatabaseException
    {
        // noop
    }

    @Override
    public void delete(Role dataRef) throws DatabaseException
    {
        // noop
    }

    @Override
    public SingleColumnDatabaseDriver<Role, SecurityType> getDomainDriver()
    {
        return noopDomainDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<Role, Boolean> getRoleEnabledDriver()
    {
        return noopRoleEnabled;
    }
}
