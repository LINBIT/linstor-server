package com.linbit.linstor.security;

import com.linbit.linstor.dbdrivers.AbsSatelliteDbDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecIdRoleDatabaseDriver;
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SatelliteSecIdRoleDbDriver
    extends AbsSatelliteDbDriver<Pair<Identity, Role>>
    implements SecIdRoleDatabaseDriver
{
    @Inject
    public SatelliteSecIdRoleDbDriver()
    {
        // no-op
    }
}
