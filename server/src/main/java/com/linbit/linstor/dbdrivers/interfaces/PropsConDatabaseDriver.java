package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.propscon.Props;

import java.util.Map;
import java.util.Set;

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
     * @throws DatabaseException
     */
    Map<String, String> loadAll(String instanceName) throws DatabaseException;

    /**
     * Performs an insert or update for the given key/value pair
     *
     * @throws DatabaseException
     */
    void persist(String isntanceName, String key, String value) throws DatabaseException;

    /**
     * Performs an insert or update for the given key/value pairs
     *
     * @throws DatabaseException
     */
    void persist(String isntanceName, Map<String, String> props) throws DatabaseException;

    /**
     * Removes the given key from the database
     *
     * @throws DatabaseException
     */
    void remove(String isntanceName, String key) throws DatabaseException;

    /**
     * Removes the given keys from the database
     *
     * @throws DatabaseException
     */
    void remove(String isntanceName, Set<String> keys) throws DatabaseException;

    /**
     * Removes all entries of this instance from the database
     *
     * @throws DatabaseException
     */
    void removeAll(String isntanceName) throws DatabaseException;
}
