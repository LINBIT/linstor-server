package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.objects.StorPoolData;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.transaction.TransactionMgr;

/**
 * Database driver for {@link com.linbit.linstor.core.objects.StorPoolData}.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface StorPoolDataDatabaseDriver
{
    /**
     * Persists the given {@link com.linbit.linstor.core.objects.StorPoolData} into the database.
     *
     * @param storPool
     *  The data to be stored (including the primary key)
     *
     * @throws DatabaseException
     */
    void create(StorPoolData storPool) throws DatabaseException;

    /**
     * Removes the given {@link com.linbit.linstor.core.objects.StorPoolData} from the database.
     *
     * @param storPool
     *  The data identifying the row to delete
     *
     * @throws DatabaseException
     */
    void delete(StorPoolData storPool) throws DatabaseException;

    /**
     * Checks if the stored primary key already exists in the database.
     * If it does not exist, {@link StorPoolDefinitionDataDatabaseDriver#create(StorPoolData, TransactionMgr)}
     * is called.
     *
     * @param storPool
     *  The data identifying the row to exist
     *
     * @throws DatabaseException
     */
    void ensureEntryExists(StorPoolData storPool)
        throws DatabaseException;
}
