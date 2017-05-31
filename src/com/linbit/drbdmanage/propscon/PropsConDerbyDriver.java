package com.linbit.drbdmanage.propscon;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.linbit.drbdmanage.dbcp.DbConnectionPool;

public class PropsConDerbyDriver implements PropsConDatabaseDriver
{
    private static final String MERGE_TABLE_NAME = "MERGE_PROPS_CONTAINER";
    public static final String PROPSCON_TABLE_NAME = "PROPS_CONTAINERS";
    public static final String PROPSCON_COL_INSTANCE= "PROPS_INSTANCE";
    public static final String PROPSCON_COL_KEY = "PROP_KEY";
    public static final String PROPSCON_COL_VALUE = "PROP_VALUE";

    public static final String CREATE_TABLE =
        "CREATE TABLE " + PROPSCON_TABLE_NAME + "\n" +
        "(\n" +
        "    " + PROPSCON_COL_INSTANCE + " VARCHAR(512) NOT NULL \n" +
        "        CONSTRAINT PRP_INST_CHKNAME CHECK(UPPER(" + PROPSCON_COL_INSTANCE + ") = " + PROPSCON_COL_INSTANCE +
        "            AND LENGTH(" + PROPSCON_COL_INSTANCE + ") >= 2),\n" +
        "    " + PROPSCON_COL_KEY + " VARCHAR(512) NOT NULL, \n" +
        "    " + PROPSCON_COL_VALUE + " VARCHAR(512) NOT NULL, \n " +
        "    PRIMARY KEY (" + PROPSCON_COL_INSTANCE + ", " + PROPSCON_COL_KEY + ")\n" +
        ")";
    private static final String MERGE_STATEMENT =
        "MERGE INTO PROPS_CONTAINERS AS DST "+
        "USING MERGE_PROPS_CONTAINER AS SRC "+
        "ON DST.PROPS_INSTANCE = ? AND DST.PROP_KEY = ? "+
        "WHEN MATCHED THEN UPDATE SET PROP_VALUE = ? "+
        "WHEN NOT MATCHED THEN INSERT VALUES (?, ?, ?)";

    private static final String SELECT_ALL_ENTRIES_BY_INSTANCE =
        "SELECT " + PROPSCON_COL_KEY + ", " + PROPSCON_COL_VALUE +
        " FROM " + PROPSCON_TABLE_NAME +
        " WHERE " + PROPSCON_COL_INSTANCE + " = ?";

    private static final String REMOVE_ENTRY =
        "DELETE FROM " + PROPSCON_TABLE_NAME +
        "    WHERE " + PROPSCON_COL_INSTANCE + " = ? " +
        "        AND " + PROPSCON_COL_KEY + " = ?";

    private static final String REMOVE_ALL_ENTRIES =
        "DELETE FROM " + PROPSCON_TABLE_NAME +
        "    WHERE " + PROPSCON_COL_INSTANCE + " = ? ";

    private static final String CREATE_MERGE_TABLE =
        "CREATE TABLE " + MERGE_TABLE_NAME + " " +
        " (COL SMALLINT NOT NULL)";
    private static final String INSERT_INTO_MERGE =
        "INSERT INTO " + MERGE_TABLE_NAME + " VALUES ( ? )";
    private static final String SELECT_FROM_MERGE =
        "SELECT * FROM " + MERGE_TABLE_NAME;

    private final String instanceName;
    private final DbConnectionPool pool;

    public PropsConDerbyDriver(String instanceName, DbConnectionPool pool) throws SQLException
    {
        this.instanceName = instanceName;
        this.pool = pool;

        Connection connection = pool.getConnection();

        try
        {
            PreparedStatement stmt = connection.prepareStatement(CREATE_TABLE);
            stmt.executeUpdate();
            stmt.close();
        }
        catch (SQLException sqlExc)
        {
            if (sqlExc.getSQLState().equals("X0Y32"))
            {
                // table already exists - ignore this exception
            }
            else
            {
                throw sqlExc;
            }
        }

        try
        {
            PreparedStatement stmt = connection.prepareStatement(CREATE_MERGE_TABLE);
            stmt.executeUpdate();
            stmt.close();
        }
        catch (SQLException sqlExc)
        {
            if (sqlExc.getSQLState().equals("X0Y32"))
            {
                // table already exists - ignore this exception
            }
            else
            {
                throw sqlExc;
            }
        }

        try
        {
            PreparedStatement stmt = connection.prepareStatement(SELECT_FROM_MERGE);
            ResultSet resultSet = stmt.executeQuery();
            if (!resultSet.next())
            {
                // ensure we have at least one element in the merge table
                // (otherwise the MERGE sql-op does not do anything)
                stmt.close();
                stmt = connection.prepareStatement(INSERT_INTO_MERGE);
                stmt.setInt(1, 42);
                stmt.executeUpdate();
                stmt.close();
            }
        }
        catch (SQLException sqlExc)
        {
            throw sqlExc;
        }


        pool.returnConnection(connection);
    }

    @Override
    public void persist(String key, String value) throws SQLException
    {
        Connection dbConn = pool.getConnection();
        dbConn.setAutoCommit(false);
        PreparedStatement stmt = dbConn.prepareStatement(MERGE_STATEMENT);

        stmt.setString(1, instanceName);
        stmt.setString(2, key);
        stmt.setString(3, value);
        stmt.setString(4, instanceName);
        stmt.setString(5, key);
        stmt.setString(6, value);

        stmt.executeUpdate();

        dbConn.commit();
        stmt.close();
        pool.returnConnection(dbConn);
    }

    @Override
    public void persist(Map<String, String> props) throws SQLException
    {
        Connection dbConn = pool.getConnection();
        dbConn.setAutoCommit(false);
        PreparedStatement stmt = dbConn.prepareStatement(MERGE_STATEMENT);

        for (Entry<String, String> entry : props.entrySet())
        {
            String key = entry.getKey();
            String value = entry.getValue();

            stmt.setString(1, instanceName);
            stmt.setString(2, key);
            stmt.setString(3, value);
            stmt.setString(4, instanceName);
            stmt.setString(5, key);
            stmt.setString(6, value);

            stmt.executeUpdate();
        }

        dbConn.commit();
        stmt.close();
        pool.returnConnection(dbConn);
    }

    @Override
    public void remove(String key) throws SQLException
    {
        Connection dbConn = pool.getConnection();
        dbConn.setAutoCommit(false);
        PreparedStatement stmt = dbConn.prepareStatement(REMOVE_ENTRY);

        stmt.setString(1, instanceName);
        stmt.setString(2, key);

        stmt.executeUpdate();
        dbConn.commit();
        stmt.close();
        pool.returnConnection(dbConn);
    }

    @Override
    public void remove(Set<String> keys) throws SQLException
    {
        Connection dbConn = pool.getConnection();
        dbConn.setAutoCommit(false);
        PreparedStatement stmt = dbConn.prepareStatement(REMOVE_ENTRY);

        stmt.setString(1, instanceName);

        for (String key : keys)
        {
            stmt.setString(2, key);
            stmt.executeUpdate();
        }
        dbConn.commit();
        stmt.close();
        pool.returnConnection(dbConn);
    }

    @Override
    public void removeAll() throws SQLException
    {
        Connection dbConn = pool.getConnection();
        dbConn.setAutoCommit(false);
        PreparedStatement stmt = dbConn.prepareStatement(REMOVE_ALL_ENTRIES);

        stmt.setString(1, instanceName);
        stmt.executeUpdate();

        dbConn.commit();
        stmt.close();
        pool.returnConnection(dbConn);
    }

    @Override
    public Map<String, String> load() throws SQLException
    {
        Connection dbConn = pool.getConnection();
        PreparedStatement stmt = dbConn.prepareStatement(SELECT_ALL_ENTRIES_BY_INSTANCE);

        stmt.setString(1, instanceName);

        ResultSet resultSet = stmt.executeQuery();
        Map<String, String> map = new HashMap<>();

        while (resultSet.next())
        {
            String key = resultSet.getString(1);
            String value = resultSet.getString(2);

            map.put(key, value);
        }
        resultSet.close();
        stmt.close();
        pool.returnConnection(dbConn);
        return map;
    }

    @Override
    public String getInstanceName()
    {
        return instanceName;
    }
}
