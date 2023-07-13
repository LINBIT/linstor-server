package com.linbit.linstor.security;

import com.linbit.linstor.dbdrivers.AbsSatelliteDbDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecObjProtAclDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SatelliteSecObjProtAclDbDriver
    extends AbsSatelliteDbDriver<AccessControlEntry>
    implements SecObjProtAclDatabaseDriver
{
    private final SingleColumnDatabaseDriver<AccessControlEntry, AccessType> noopAccessTypeDriver;

    @Inject
    public SatelliteSecObjProtAclDbDriver()
    {
        noopAccessTypeDriver = getNoopColumnDriver();
    }

    @Override
    public SingleColumnDatabaseDriver<AccessControlEntry, AccessType> getAccessTypeDriver()
    {
        return noopAccessTypeDriver;
    }
}
