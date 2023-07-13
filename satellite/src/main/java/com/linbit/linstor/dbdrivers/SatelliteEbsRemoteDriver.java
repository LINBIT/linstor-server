package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.core.objects.remotes.EbsRemote;
import com.linbit.linstor.dbdrivers.interfaces.remotes.EbsRemoteDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.net.URL;

@Singleton
public class SatelliteEbsRemoteDriver
    extends AbsSatelliteDbDriver<EbsRemote>
    implements EbsRemoteDatabaseDriver
{
    private final SingleColumnDatabaseDriver<EbsRemote, URL> urlDriver;
    private final SingleColumnDatabaseDriver<EbsRemote, String> availabilityZoneDriver;
    private final SingleColumnDatabaseDriver<EbsRemote, String> regionDriver;
    private final SingleColumnDatabaseDriver<EbsRemote, byte[]> encryptedAccessKeyDriver;
    private final SingleColumnDatabaseDriver<EbsRemote, byte[]> encryptedSecretKeyDriver;
    private final StateFlagsPersistence<EbsRemote> stateFlagsDriver;

    @Inject
    public SatelliteEbsRemoteDriver()
    {
        stateFlagsDriver = getNoopFlagDriver();
        urlDriver = getNoopColumnDriver();
        availabilityZoneDriver = getNoopColumnDriver();
        regionDriver = getNoopColumnDriver();
        encryptedAccessKeyDriver = getNoopColumnDriver();
        encryptedSecretKeyDriver = getNoopColumnDriver();
    }

    @Override
    public SingleColumnDatabaseDriver<EbsRemote, URL> getUrlDriver()
    {
        return urlDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<EbsRemote, String> getAvailabilityZoneDriver()
    {
        return availabilityZoneDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<EbsRemote, String> getRegionDriver()
    {
        return regionDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<EbsRemote, byte[]> getEncryptedAccessKeyDriver()
    {
        return encryptedAccessKeyDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<EbsRemote, byte[]> getEncryptedSecretKeyDriver()
    {
        return encryptedSecretKeyDriver;
    }

    @Override
    public StateFlagsPersistence<EbsRemote> getStateFlagsPersistence()
    {
        return stateFlagsDriver;
    }
}
