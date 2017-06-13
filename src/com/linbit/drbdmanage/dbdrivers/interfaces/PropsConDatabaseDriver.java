package com.linbit.drbdmanage.dbdrivers.interfaces;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

import com.linbit.drbdmanage.propscon.PropsContainer;

public interface PropsConDatabaseDriver
{
    /**
     * Returns the instance name of the {@link PropsContainer}
     */
    String getInstanceName();

    /**
     * Returns a map containing all properties of the current instance
     *
     * @param con
     * @throws SQLException
     */
    Map<String, String> load(Connection con) throws SQLException;

    /**
     * Performs an insert or update for the given key/value pair
     */
    void persist(Connection con, String key, String value) throws SQLException;

    /**
     * Performs an insert or update for the given key/value pairs
     */
    void persist(Connection con, Map<String, String> props) throws SQLException;

    /**
     * Removes the given key from the database
     */
    void remove(Connection con, String key) throws SQLException;

    /**
     * Removes the given keys from the database
     */
    void remove(Connection con, Set<String> keys) throws SQLException;

    /**
     * Removes all entries of this instance from the database
     */
    void removeAll(Connection con) throws SQLException;
}
