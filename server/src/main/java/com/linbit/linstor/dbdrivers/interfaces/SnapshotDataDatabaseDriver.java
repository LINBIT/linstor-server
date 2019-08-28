package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotData;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

/**
 * Database driver for {@link Snapshot}.
 */
public interface SnapshotDataDatabaseDriver extends GenericDatabaseDriver<Snapshot>
{

    /**
     * A special sub-driver to update the persisted flags.
     */
    StateFlagsPersistence<Snapshot> getStateFlagsPersistence();
}
