package com.linbit.linstor.dbdrivers.interfaces;

import java.sql.SQLException;

import com.linbit.linstor.StorPoolDefinitionData;
import com.linbit.linstor.StorPoolName;

/**
 * Database driver for {@link com.linbit.linstor.StorPoolDefinitionData}.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface StorPoolDefinitionDataDatabaseDriver
{
    /**
     * Loads the {@link com.linbit.linstor.StorPoolDefinitionData} specified by the parameter {@code storPoolName}
     *
     * @param storPoolName
     *  The primaryKey identifying the row to load
     * @param logWarnIfNotExists
     *  If true a warning is logged if the requested entry does not exist
     *
     * @throws SQLException
     */
    StorPoolDefinitionData load(StorPoolName storPoolName, boolean logWarnIfNotExists)
        throws SQLException;

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
}
