package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.CollectionDatabaseDriver;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.core.objects.ResourceDefinitionData;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;

/**
 * Database driver for {@link ResourceDefinitionData}.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface ResourceDefinitionDataDatabaseDriver
{
    /**
     * Persists the given {@link ResourceDefinitionData} into the database.
     *
     * @param resourceDefinition
     *  The data to be stored (including the primary key)
     *
     * @throws DatabaseException
     */
    void create(ResourceDefinitionData resourceDefinition) throws DatabaseException;

    /**
     * Removes the given {@link ResourceDefinitionData} from the database
     *
     * @param resourceDefinition
     *  The data identifying the row to delete
     *
     * @throws DatabaseException
     */
    void delete(ResourceDefinitionData resourceDefinition) throws DatabaseException;

    /**
     * A special sub-driver to update the persisted flags
     */
    StateFlagsPersistence<ResourceDefinitionData> getStateFlagsPersistence();

    /**
     * Checks if the stored primary key already exists in the database.
     *
     * @param resourceName
     *  The primary key specifying the database entry
     *
     * @return
     *  True if the data record exists. False otherwise.
     * @throws DatabaseException
     */
    boolean exists(ResourceName resourceName) throws DatabaseException;

    /**
     * A special sub-driver to update the layer stack
     */
    CollectionDatabaseDriver<ResourceDefinitionData, DeviceLayerKind> getLayerStackDriver();

}
