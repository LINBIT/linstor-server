package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

/**
 * Database driver for {@link SnapshotDefinition}.
 */
public interface SnapshotDefinitionDataDatabaseDriver extends GenericDatabaseDriver<SnapshotDefinition>
{

    /**
     * A special sub-driver to update the persisted flags.
     */
    StateFlagsPersistence<SnapshotDefinition> getStateFlagsPersistence();
}
