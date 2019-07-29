package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.StorPoolDefinitionData;
import com.linbit.linstor.dbdrivers.DatabaseException;

/**
 * Database driver for {@link com.linbit.linstor.StorPoolDefinitionData}.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface StorPoolDefinitionDataDatabaseDriver
{
    /**
     * Persists the given {@link com.linbit.linstor.StorPoolDefinitionData} into the database.
     *
     * @param storPoolDefinition
     *  The data to be stored (including the primary key)
     *
     * @throws DatabaseException
     */
    void create(StorPoolDefinitionData storPoolDefinition) throws DatabaseException;

    /**
     * Removes the given {@link com.linbit.linstor.StorPoolDefinitionData} from the database.
     *
     * @param storPoolDefinition
     *  The data identifying the row to delete
     *
     * @throws DatabaseException
     */
    void delete(StorPoolDefinitionData storPoolDefinition) throws DatabaseException;

    StorPoolDefinitionData createDefaultDisklessStorPool() throws DatabaseException;
}
