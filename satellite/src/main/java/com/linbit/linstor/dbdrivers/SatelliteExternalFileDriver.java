package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.core.objects.ExternalFile;
import com.linbit.linstor.dbdrivers.interfaces.ExternalFileDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

import javax.inject.Inject;

public class SatelliteExternalFileDriver implements ExternalFileDatabaseDriver
{
    private final StateFlagsPersistence<?> stateFlagsDriver = new SatelliteFlagDriver();
    private final SingleColumnDatabaseDriver<?, ?> singleColDriver = new SatelliteSingleColDriver<>();

    @Inject
    public SatelliteExternalFileDriver()
    {
    }

    @Override
    public void create(ExternalFile fileRef) throws DatabaseException
    {
        // no-op
    }

    @Override
    public void delete(ExternalFile fileRef) throws DatabaseException
    {
        // no-op
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<ExternalFile, byte[]> getContentDriver()
    {
        return (SingleColumnDatabaseDriver<ExternalFile, byte[]>) singleColDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<ExternalFile, byte[]> getContentCheckSumDriver()
    {
        return (SingleColumnDatabaseDriver<ExternalFile, byte[]>) singleColDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public StateFlagsPersistence<ExternalFile> getStateFlagPersistence()
    {
        return (StateFlagsPersistence<ExternalFile>) stateFlagsDriver;
    }

}
