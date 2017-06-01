package com.linbit.drbdmanage.propscon;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.linbit.drbdmanage.dbcp.DbConnectionPool;

public class DerbyDriverPropsConBase
{
    private static final String DB_URL = "jdbc:derby:directory:database";
    private static final String DB_USER = "drbdmanage";
    private static final String DB_PASSWORD = "linbit";
    private static final Properties DB_PROPS = new Properties();

    private static final String TABLE_NAME = PropsConDerbyDriver.PROPSCON_TABLE_NAME;
//    private static final String COL_INSTANCE = PropsConDerbyDriver.PROPSCON_COL_INSTANCE;
    private static final String COL_KEY = PropsConDerbyDriver.PROPSCON_COL_KEY;
//    private static final String COL_VALUE = PropsConDerbyDriver.PROPSCON_COL_VALUE;

    private static final String CREATE_TABLE = PropsConDerbyDriver.CREATE_TABLE;
    private static final String DROP_TABLE =
        "DROP TABLE " + TABLE_NAME;
    private static final String SELECT_ALL =
        "SELECT * FROM " + TABLE_NAME;
    private static final String INSERT =
        "INSERT INTO " + TABLE_NAME + " VALUES (?, ?, ?)";
    private static final String DELETE =
        "DELETE FROM " + TABLE_NAME + " WHERE " + COL_KEY + " = ?";

    protected static final String DEFAULT_INSTANCE_NAME = "DEFAULT_INSTANCE";

    private List<Statement> statements = new ArrayList<>();

    private Connection connection;
    protected PropsConDerbyDriver dbDriver;
    protected static DbConnectionPool dbConnPool;

    @SuppressWarnings("unused")
    @BeforeClass
    public static void setUpBeforeClass() throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException
    {
        // load the clientDriver...
        Class.forName("org.apache.derby.jdbc.ClientDriver").newInstance();
        DB_PROPS.setProperty("create", "true");
        DB_PROPS.setProperty("user", DB_USER);
        DB_PROPS.setProperty("password", DB_PASSWORD);

        dbConnPool = new DbConnectionPool();
        dbConnPool.initializeDataSource(DB_URL, DB_PROPS);

        // create a dummy driver, just to test if the DB is available
        // fail fast, not for each testcase
        try
        {
            new PropsConDerbyDriver(DEFAULT_INSTANCE_NAME, dbConnPool);
        }
        catch (SQLException sqlExc)
        {
            SQLException cause = sqlExc;
            while (cause != null)
            {
                if ("XSDB6".equals(cause.getSQLState()))
                {
                    // another instance is using the db...
                    fail(cause.getLocalizedMessage());
                }
                cause = cause.getNextException();
            }
            throw sqlExc;
        }
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
        dbConnPool.shutdown();
    }

    @Before
    public void setUp() throws SQLException
    {
        dbDriver = new PropsConDerbyDriver(DEFAULT_INSTANCE_NAME, dbConnPool);

        connection = dbConnPool.getConnection();
        createTable(true);
    }

    @After
    public void tearDown() throws SQLException
    {
        for (Statement statement : statements)
        {
            statement.close();
        }
        dropTable();
        connection.close();
    }

    private void createTable(boolean dropIfExists) throws SQLException
    {
        try (Connection con = dbConnPool.getConnection())
        {
            try (Statement statement = con.createStatement())
            {
                statement.executeUpdate(CREATE_TABLE);
            }
            catch (SQLException sqlExc)
            {
                if ("X0Y32".equals(sqlExc.getSQLState())) // table already exists
                {
                    if (dropIfExists)
                    {
                        dropTable();
                        createTable(false);
                    }
                    else
                    {
                        throw sqlExc;
                    }
                }
                else
                {
                    throw sqlExc;
                }
            }
        }
    }

    private void dropTable() throws SQLException
    {
        try (Connection con = dbConnPool.getConnection())
        {
            try (Statement statement = con.createStatement())
            {
                statement.executeUpdate(DROP_TABLE);
            }
        }
    }

    protected String debugGetAllContent() throws SQLException
    {
        ResultSet allContent = getAllContent();
        StringBuilder sb = new StringBuilder();
        while (allContent.next())
        {
            sb.append(allContent.getString(1)).append(": ")
                .append(allContent.getString(2)).append(" = ")
                .append(allContent.getString(3)).append("\n");
        }
        return sb.toString();
    }

    protected ResultSet getAllContent() throws SQLException
    {
        PreparedStatement preparedStatement = connection.prepareStatement(SELECT_ALL);
        statements.add(preparedStatement);
        return preparedStatement.executeQuery();
    }

    protected void checkIfPresent(Map<String, String> origMap, String expectedInstanceName) throws SQLException
    {
        Map<String, String> map = new HashMap<>(origMap);
        ResultSet resultSet = getAllContent();
        while (resultSet.next())
        {
            String instanceName = resultSet.getString(1);
            if (expectedInstanceName.equals(instanceName)) // not an assert as db may contain multiple containers
            {
                String key = resultSet.getString(2);
                String value = resultSet.getString(3);

                assertTrue("Unexpected key [" + key + "]", map.containsKey(key));
                String expectedValue = map.remove(key);
                assertEquals("Unexpected value [" + value + "]", expectedValue, value);
            }
        }

        assertTrue("Database does not contain all expected entries", map.isEmpty());
    }

    protected void insert(String instanceName, String key, String value) throws SQLException
    {
        try (Connection con = dbConnPool.getConnection())
        {
            try (PreparedStatement preparedStatement = con.prepareStatement(INSERT))
            {
                preparedStatement.setString(1, instanceName);
                preparedStatement.setString(2, key);
                preparedStatement.setString(3, value);
                preparedStatement.executeUpdate();
            }
        }
    }

    protected void insert(String instanceName, Map<String, String> map) throws SQLException
    {
        try (Connection con = dbConnPool.getConnection())
        {
            try (PreparedStatement preparedStatement = con.prepareStatement(INSERT))
            {
                preparedStatement.setString(1, instanceName);

                Set<Entry<String,String>> entrySet = map.entrySet();
                for (Entry<String, String> entry : entrySet)
                {
                    preparedStatement.setString(2, entry.getKey());
                    preparedStatement.setString(3, entry.getValue());
                    preparedStatement.executeUpdate();
                }
            }
        }
    }

    protected void checkExpectedMap(Map<String, String> expectedMap, Props props)
    {
        assertEquals("Unexpected entries in PropsContainer", expectedMap.size() , props.size());

        Set<Entry<String,String>> entrySet = expectedMap.entrySet();
        // we use this map so we do not trigger props.removeProp (which triggers a DB remove)
        Map<String, String> propsMap = new HashMap<>(props.map());
        for (Entry<String, String> entry : entrySet)
        {
            String key = entry.getKey();
            String value = entry.getValue();
            assertTrue("PropsContainer missing key [" + key + "]", propsMap.keySet().contains(key));
            assertEquals("Unexpected value", value, propsMap.get(key));
            propsMap.remove(key);
        }
        assertEquals("PropsContainer contains unexpected entries", 0, propsMap.size());
    }

    protected void delete(String instanceName, String key) throws SQLException
    {
        try (Connection con = dbConnPool.getConnection())
        {
            try (PreparedStatement preparedStatement = con.prepareStatement(DELETE))
            {
                preparedStatement.setString(1, instanceName);
                preparedStatement.setString(1, key);
                preparedStatement.executeUpdate();
            }
        }
    }
}
