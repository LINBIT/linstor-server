package com.linbit.linstor.security;

import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.SecIdRoleDatabaseDriver;
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SatelliteSecIdRoleDbDriver implements SecIdRoleDatabaseDriver
{
    @Inject
    public SatelliteSecIdRoleDbDriver()
    {
    }

    @Override
    public void create(Pair<Identity, Role> dataRef) throws DatabaseException
    {
        // noop
    }

    @Override
    public void delete(Pair<Identity, Role> dataRef) throws DatabaseException
    {
        // noop
    }
}
