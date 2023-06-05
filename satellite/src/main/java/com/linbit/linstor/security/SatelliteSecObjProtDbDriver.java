package com.linbit.linstor.security;

import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.SatelliteSingleColDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecObjProtDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SatelliteSecObjProtDbDriver implements SecObjProtDatabaseDriver
{
    private final SingleColumnDatabaseDriver<ObjectProtection, Role> noopRoleDriver;
    private final SingleColumnDatabaseDriver<ObjectProtection, Identity> noopIdDriver;
    private final SingleColumnDatabaseDriver<ObjectProtection, SecurityType> noopSecTypeDriver;

    @Inject
    public SatelliteSecObjProtDbDriver()
    {
        noopRoleDriver = new SatelliteSingleColDriver<>();
        noopIdDriver = new SatelliteSingleColDriver<>();
        noopSecTypeDriver = new SatelliteSingleColDriver<>();
    }

    @Override
    public void create(ObjectProtection dataRef) throws DatabaseException
    {
        // noop
    }

    @Override
    public void delete(ObjectProtection dataRef) throws DatabaseException
    {
        // noop
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
