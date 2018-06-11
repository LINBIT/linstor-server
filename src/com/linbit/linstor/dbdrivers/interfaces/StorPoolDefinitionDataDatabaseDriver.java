package com.linbit.linstor.dbdrivers.interfaces;

import java.sql.SQLException;

import com.linbit.linstor.StorPoolDefinitionData;

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
     * @throws SQLException
     */
    void create(StorPoolDefinitionData storPoolDefinition) throws SQLException;

    /**
     * Removes the given {@link com.linbit.linstor.StorPoolDefinitionData} from the database.
     *
     * @param storPoolDefinition
     *  The data identifying the row to delete
     *
     * @throws SQLException
     */
    void delete(StorPoolDefinitionData storPoolDefinition) throws SQLException;

    StorPoolDefinitionData createDefaultDisklessStorPool() throws SQLException;
}
