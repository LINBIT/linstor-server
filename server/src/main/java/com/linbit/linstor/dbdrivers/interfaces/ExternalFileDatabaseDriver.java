package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.objects.ExternalFile;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

public interface ExternalFileDatabaseDriver extends GenericDatabaseDriver<ExternalFile>
{
    SingleColumnDatabaseDriver<ExternalFile, byte[]> getContentDriver();

    SingleColumnDatabaseDriver<ExternalFile, byte[]> getContentCheckSumDriver();

    StateFlagsPersistence<ExternalFile> getStateFlagPersistence();
}
