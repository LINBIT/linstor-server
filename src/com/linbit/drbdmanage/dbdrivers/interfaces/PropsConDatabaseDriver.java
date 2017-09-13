package com.linbit.drbdmanage.dbdrivers.interfaces;

import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.propscon.Props;

/**
 * Database driver for {@link Props}.
 *
 * @author Gabor Hernadi <gabor.hernadi@linbit.com>
 */
public interface PropsConDatabaseDriver
{
    /**
     * Returns a map containing all properties of the current instance
     *
     * @throws SQLException
     */
    Map<String, String> load(String instanceName, TransactionMgr transMgr) throws SQLException;

    /**
     * Performs an insert or update for the given key/value pair
     *
     * @throws SQLException
     */
    void persist(String isntanceName, String key, String value, TransactionMgr transMgr) throws SQLException;

    /**
     * Performs an insert or update for the given key/value pairs
     *
     * @throws SQLException
     */
    void persist(String isntanceName, Map<String, String> props, TransactionMgr transMgr) throws SQLException;

    /**
     * Removes the given key from the database
     *
     * @throws SQLException
     */
    void remove(String isntanceName, String key, TransactionMgr transMgr) throws SQLException;

    /**
     * Removes the given keys from the database
     *
     * @throws SQLException
     */
    void remove(String isntanceName, Set<String> keys, TransactionMgr transMgr) throws SQLException;

    /**
     * Removes all entries of this instance from the database
     *
     * @throws SQLException
     */
    void removeAll(String isntanceName, TransactionMgr transMgr) throws SQLException;
}
