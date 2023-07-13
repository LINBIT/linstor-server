package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.core.objects.ExternalFile;
import com.linbit.linstor.dbdrivers.interfaces.ExternalFileDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

import javax.inject.Inject;

public class SatelliteExternalFileDriver
    extends AbsSatelliteDbDriver<ExternalFile>
    implements ExternalFileDatabaseDriver
{
    private final SingleColumnDatabaseDriver<ExternalFile, byte[]> contentDriver;
    private final SingleColumnDatabaseDriver<ExternalFile, byte[]> contentCheckSumDriver;
    private final StateFlagsPersistence<ExternalFile> stateFlagsDriver;

    @Inject
    public SatelliteExternalFileDriver()
    {
        contentDriver = getNoopColumnDriver();
        contentCheckSumDriver = getNoopColumnDriver();
        stateFlagsDriver = getNoopFlagDriver();
    }

    @Override
    public SingleColumnDatabaseDriver<ExternalFile, byte[]> getContentDriver()
    {
        return contentDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<ExternalFile, byte[]> getContentCheckSumDriver()
    {
        return contentCheckSumDriver;
    }

    @Override
    public StateFlagsPersistence<ExternalFile> getStateFlagPersistence()
    {
        return stateFlagsDriver;
    }
}
