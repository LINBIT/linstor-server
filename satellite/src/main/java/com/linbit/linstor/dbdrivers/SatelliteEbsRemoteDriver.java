package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.core.objects.remotes.EbsRemote;
import com.linbit.linstor.dbdrivers.interfaces.remotes.EbsRemoteDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.dbdrivers.noop.NoOpFlagDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

import javax.inject.Inject;

import java.net.URL;

public class SatelliteEbsRemoteDriver implements EbsRemoteDatabaseDriver
{
    private final StateFlagsPersistence<?> stateFlagsDriver = new NoOpFlagDriver();
    private final SingleColumnDatabaseDriver<?, ?> singleColDriver = new SatelliteSingleColDriver<>();

    @Inject
    public SatelliteEbsRemoteDriver()
    {
    }

    @Override
    public void create(EbsRemote remoteRef) throws DatabaseException
    {
        // no-op
    }

    @Override
    public void delete(EbsRemote remoteRef) throws DatabaseException
    {
        // no-op
    }

    @Override
    public SingleColumnDatabaseDriver<EbsRemote, URL> getUrlDriver()
    {
        return (SingleColumnDatabaseDriver<EbsRemote, URL>) singleColDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<EbsRemote, String> getAvailabilityZoneDriver()
    {
        return (SingleColumnDatabaseDriver<EbsRemote, String>) singleColDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<EbsRemote, String> getRegionDriver()
    {
        return (SingleColumnDatabaseDriver<EbsRemote, String>) singleColDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<EbsRemote, byte[]> getEncryptedAccessKeyDriver()
    {
        return (SingleColumnDatabaseDriver<EbsRemote, byte[]>) singleColDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SingleColumnDatabaseDriver<EbsRemote, byte[]> getEncryptedSecretKeyDriver()
    {
        return (SingleColumnDatabaseDriver<EbsRemote, byte[]>) singleColDriver;
    }

    @SuppressWarnings("unchecked")
    @Override
    public StateFlagsPersistence<EbsRemote> getStateFlagsPersistence()
    {
        return (StateFlagsPersistence<EbsRemote>) stateFlagsDriver;
    }
}
