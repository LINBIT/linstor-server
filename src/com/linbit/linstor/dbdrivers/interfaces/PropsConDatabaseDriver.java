package com.linbit.linstor.dbdrivers.interfaces;

import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

import com.linbit.linstor.propscon.Props;

/**
 * Database driver for {@link Props}.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface PropsConDatabaseDriver
{
    /**
     * Returns a map containing all properties of the current instance
     *
     * @throws SQLException
     */
    Map<String, String> loadAll(String instanceName) throws SQLException;

    /**
     * Performs an insert or update for the given key/value pair
     *
     * @throws SQLException
     */
    void persist(String isntanceName, String key, String value) throws SQLException;

    /**
     * Performs an insert or update for the given key/value pairs
     *
     * @throws SQLException
     */
    void persist(String isntanceName, Map<String, String> props) throws SQLException;

    /**
     * Removes the given key from the database
     *
     * @throws SQLException
     */
    void remove(String isntanceName, String key) throws SQLException;

    /**
     * Removes the given keys from the database
     *
     * @throws SQLException
     */
    void remove(String isntanceName, Set<String> keys) throws SQLException;

    /**
     * Removes all entries of this instance from the database
     *
     * @throws SQLException
     */
    void removeAll(String isntanceName) throws SQLException;
}
