package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

/**
 * Database driver for {@link Snapshot}.
 */
public interface SnapshotDatabaseDriver
    extends AbsResourceDatabaseDriver<Snapshot>, GenericDatabaseDriver<AbsResource<Snapshot>>
{

    /**
     * A special sub-driver to update the persisted flags.
     */
    StateFlagsPersistence<AbsResource<Snapshot>> getStateFlagsPersistence();
}
