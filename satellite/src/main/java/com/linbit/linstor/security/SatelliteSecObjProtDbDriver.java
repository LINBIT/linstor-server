package com.linbit.linstor.security;

import com.linbit.linstor.dbdrivers.AbsSatelliteDbDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecObjProtDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SatelliteSecObjProtDbDriver
    extends AbsSatelliteDbDriver<ObjectProtection>
    implements SecObjProtDatabaseDriver
{
    private final SingleColumnDatabaseDriver<ObjectProtection, Role> noopRoleDriver;
    private final SingleColumnDatabaseDriver<ObjectProtection, Identity> noopIdDriver;
    private final SingleColumnDatabaseDriver<ObjectProtection, SecurityType> noopSecTypeDriver;

    @Inject
    public SatelliteSecObjProtDbDriver()
    {
        noopRoleDriver = getNoopColumnDriver();
        noopIdDriver = getNoopColumnDriver();
        noopSecTypeDriver = getNoopColumnDriver();
    }

    @Override
    public SingleColumnDatabaseDriver<ObjectProtection, Role> getOwnerRoleDriver()
    {
        return noopRoleDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<ObjectProtection, Identity> getCreatorIdentityDriver()
    {
        return noopIdDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<ObjectProtection, SecurityType> getSecurityTypeDriver()
    {
        return noopSecTypeDriver;
    }
}
