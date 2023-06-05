package com.linbit.linstor.security;

import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.SatelliteSingleColDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecObjProtAclDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SatelliteSecObjProtAclDbDriver implements SecObjProtAclDatabaseDriver
{
    private final SingleColumnDatabaseDriver<AccessControlEntry, AccessType> noopAccessTypeDriver;

    @Inject
    public SatelliteSecObjProtAclDbDriver()
    {
        noopAccessTypeDriver = new SatelliteSingleColDriver<>();
    }

    @Override
    public void create(AccessControlEntry dataRef) throws DatabaseException
    {
        // noop
    }

    @Override
    public void delete(AccessControlEntry dataRef) throws DatabaseException
    {
        // noop
    }

    @Override
    public SingleColumnDatabaseDriver<AccessControlEntry, AccessType> getAccessTypeDriver()
    {
        return noopAccessTypeDriver;
    }
}
