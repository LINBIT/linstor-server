package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.updater.CollectionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;

/**
 * Database driver for {@link ResourceDefinition}.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface ResourceDefinitionDatabaseDriver
{
    /**
     * Persists the given {@link ResourceDefinition} into the database.
     *
     * @param resourceDefinition
     *  The data to be stored (including the primary key)
     *
     * @throws DatabaseException
     */
    void create(ResourceDefinition resourceDefinition) throws DatabaseException;

    /**
     * Removes the given {@link ResourceDefinition} from the database
     *
     * @param resourceDefinition
     *  The data identifying the row to delete
     *
     * @throws DatabaseException
     */
    void delete(ResourceDefinition resourceDefinition) throws DatabaseException;

    /**
     * A special sub-driver to update the persisted flags
     */
    StateFlagsPersistence<ResourceDefinition> getStateFlagsPersistence();


    /**
     * A special sub-driver to update the layer stack
     */
    CollectionDatabaseDriver<ResourceDefinition, DeviceLayerKind> getLayerStackDriver();

    SingleColumnDatabaseDriver<ResourceDefinition, ResourceGroup> getRscGrpDriver();
}
