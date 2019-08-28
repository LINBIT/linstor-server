package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.objects.SnapshotDefinitionData;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

/**
 * Database driver for {@link SnapshotDefinitionData}.
 */
public interface SnapshotDefinitionDataDatabaseDriver extends GenericDatabaseDriver<SnapshotDefinitionData>
{

    /**
     * A special sub-driver to update the persisted flags.
     */
    StateFlagsPersistence<SnapshotDefinitionData> getStateFlagsPersistence();
}
