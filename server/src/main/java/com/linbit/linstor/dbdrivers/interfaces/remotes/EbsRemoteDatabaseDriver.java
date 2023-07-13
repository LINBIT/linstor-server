package com.linbit.linstor.dbdrivers.interfaces.remotes;

import com.linbit.linstor.core.objects.remotes.EbsRemote;
import com.linbit.linstor.dbdrivers.interfaces.GenericDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

import java.net.URL;

public interface EbsRemoteDatabaseDriver extends GenericDatabaseDriver<EbsRemote>
{
    SingleColumnDatabaseDriver<EbsRemote, URL> getUrlDriver();

    SingleColumnDatabaseDriver<EbsRemote, String> getAvailabilityZoneDriver();

    SingleColumnDatabaseDriver<EbsRemote, String> getRegionDriver();

    SingleColumnDatabaseDriver<EbsRemote, byte[]> getEncryptedSecretKeyDriver();

    SingleColumnDatabaseDriver<EbsRemote, byte[]> getEncryptedAccessKeyDriver();

    StateFlagsPersistence<EbsRemote> getStateFlagsPersistence();
}
