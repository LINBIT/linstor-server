package com.linbit.linstor.security;

import com.linbit.linstor.dbdrivers.AbsSatelliteDbDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecRoleDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SatelliteSecRoleDbDriver
    extends AbsSatelliteDbDriver<Role>
    implements SecRoleDatabaseDriver
{
    private final SingleColumnDatabaseDriver<Role, SecurityType> noopDomainDriver;
    private final SingleColumnDatabaseDriver<Role, Boolean> noopRoleEnabled;

    @Inject
    public SatelliteSecRoleDbDriver()
    {
        noopDomainDriver = getNoopColumnDriver();
        noopRoleEnabled = getNoopColumnDriver();
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
