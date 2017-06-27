package com.linbit.drbdmanage.propscon;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Map.Entry;

import com.linbit.drbdmanage.dbdrivers.interfaces.PropsConDatabaseDriver;

import java.util.Set;
import java.util.TreeMap;

public class PropsConDerbyDriver implements PropsConDatabaseDriver
{
    public static final String TBL_PROP = "PROPS_CONTAINERS";
    public static final String COL_INSTANCE = "PROPS_INSTANCE";
    public static final String COL_KEY = "PROP_KEY";
    public static final String COL_VALUE = "PROP_VALUE";

    private static final String SELECT_ENTRY_FOR_UPDATE =
        "SELECT " + COL_INSTANCE + ", " + COL_KEY + ", " + COL_VALUE + "\n" +
        " FROM " + TBL_PROP + "\n" +
        " WHERE " + COL_INSTANCE + " = ? AND \n" +
        "       " + COL_KEY +      " = ? \n" +
        " FOR UPDATE OF " + COL_INSTANCE + "," + COL_KEY + ", "+ COL_VALUE;
    private static final String INSERT_ENTRY =
        "INSERT INTO " + TBL_PROP + "\n" +
        " VALUES (?, ?, ?)";

    private static final String SELECT_ALL_ENTRIES_BY_INSTANCE =
        "SELECT " + COL_KEY + ", " + COL_VALUE + "\n" +
        " FROM " + TBL_PROP + "\n" +
        " WHERE " + COL_INSTANCE + " = ?";

    private static final String REMOVE_ENTRY =
        "DELETE FROM " + TBL_PROP + "\n" +
        "    WHERE " + COL_INSTANCE + " = ? \n" +
        "        AND " + COL_KEY + " = ?";

    private static final String REMOVE_ALL_ENTRIES =
        "DELETE FROM " + TBL_PROP + "\n" +
        "    WHERE " + COL_INSTANCE + " = ? ";


    private final String instanceName;

    public PropsConDerbyDriver(String instanceName)
    {
        this.instanceName = instanceName;
    }

    @Override
    public void persist(Connection dbConn, String key, String value) throws SQLException
    {
        if (dbConn != null)
        {
            persistImpl(dbConn, key, value);
        }
    }

    @Override
    public void persist(Connection dbConn, Map<String, String> props) throws SQLException
    {
        if (dbConn != null)
        {
            for (Entry<String, String> entry : props.entrySet())
            {
                persistImpl(dbConn, entry.getKey(), entry.getValue());
            }
        }
    }

    private void persistImpl(Connection dbConn, String key, String value) throws SQLException
    {
        PreparedStatement stmt = dbConn.prepareStatement(
            SELECT_ENTRY_FOR_UPDATE,
            ResultSet.TYPE_SCROLL_SENSITIVE,
            ResultSet.CONCUR_UPDATABLE
        );

        stmt.setString(1, instanceName);
        stmt.setString(2, key);

        ResultSet resultSet = stmt.executeQuery();
        if (resultSet.next())
        {
            resultSet.updateString(3, value);
            resultSet.updateRow();
        }
        else
        {
            resultSet.moveToInsertRow();
            resultSet.updateString(1, instanceName);
            resultSet.updateString(2, key);
            resultSet.updateString(3, value);
            resultSet.insertRow();
        }

        stmt.close();
    }

    @Override
    public void remove(Connection dbConn, String key) throws SQLException
    {
        if (dbConn != null)
        {
            PreparedStatement stmt = dbConn.prepareStatement(REMOVE_ENTRY);

            stmt.setString(1, instanceName);
            stmt.setString(2, key);

            stmt.executeUpdate();
            stmt.close();
        }
    }

    @Override
    public void remove(Connection dbConn, Set<String> keys) throws SQLException
    {
        if (dbConn != null)
        {
            PreparedStatement stmt = dbConn.prepareStatement(REMOVE_ENTRY);

            stmt.setString(1, instanceName);

            for (String key : keys)
            {
                stmt.setString(2, key);
                stmt.executeUpdate();
            }
            stmt.close();
        }
    }

    @Override
    public void removeAll(Connection dbConn) throws SQLException
    {
        if (dbConn != null)
        {
            dbConn.setAutoCommit(false);
            PreparedStatement stmt = dbConn.prepareStatement(REMOVE_ALL_ENTRIES);

            stmt.setString(1, instanceName);
            stmt.executeUpdate();

            stmt.close();
        }
    }

    @Override
    public Map<String, String> load(Connection dbConn) throws SQLException
    {
        Map<String, String> ret = new TreeMap<>();
        if (dbConn != null)
        {
            PreparedStatement stmt = dbConn.prepareStatement(SELECT_ALL_ENTRIES_BY_INSTANCE);

            stmt.setString(1, instanceName);

            ResultSet resultSet = stmt.executeQuery();

            while (resultSet.next())
            {
                String key = resultSet.getString(1);
                String value = resultSet.getString(2);

                ret.put(key, value);
            }
            resultSet.close();
            stmt.close();
        }
        return ret;
    }

    @Override
    public String getInstanceName()
    {
        return instanceName;
    }
}
