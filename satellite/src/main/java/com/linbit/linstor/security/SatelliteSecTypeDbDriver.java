package com.linbit.linstor.security;

import com.linbit.linstor.dbdrivers.AbsSatelliteDbDriver;
import com.linbit.linstor.dbdrivers.interfaces.SecTypeDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SatelliteSecTypeDbDriver
    extends AbsSatelliteDbDriver<SecurityType>
    implements SecTypeDatabaseDriver
{
    private SingleColumnDatabaseDriver<SecurityType, Boolean> typeEnabledDriver;

    @Inject
    public SatelliteSecTypeDbDriver()
    {
        typeEnabledDriver = getNoopColumnDriver();
    }

    @Override
    public SingleColumnDatabaseDriver<SecurityType, Boolean> getTypeEnabledDriver()
    {
        return typeEnabledDriver;
    }
}
