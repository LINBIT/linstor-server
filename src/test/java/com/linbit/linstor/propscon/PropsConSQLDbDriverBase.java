package com.linbit.linstor.propscon;

import com.linbit.linstor.dbdrivers.PropsDbDriver;
import com.linbit.linstor.security.GenericDbBase;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.junit.Before;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PropsConSQLDbDriverBase extends GenericDbBase
{
    private static final int COL_ID_INSTANCE = 1;
    private static final int COL_ID_KEY = 2;
    private static final int COL_ID_VAL = 3;

    private static final String SELECT_ALL_PROPS =
        "SELECT * FROM " + TBL_PROPS_CONTAINERS;
    private static final String INSERT =
        "INSERT INTO " + TBL_PROPS_CONTAINERS + " VALUES (?, ?, ?)";
    private static final String DELETE =
        "DELETE FROM " + TBL_PROPS_CONTAINERS +
        " WHERE " + PROPS_INSTANCE + " = ? AND" +
        "       " + PROP_KEY + " = ?";

    protected static final String DEFAULT_INSTANCE_NAME = "DEFAULT_INSTANCE";

    @Inject
    protected PropsDbDriver dbDriver;

    @Before
    public void setUp() throws Exception
    {
        super.setUpAndEnterScope();
    }

    protected String debugGetAllProps() throws SQLException
    {
        Connection connection = getConnection();
        PreparedStatement stmt = connection.prepareStatement(SELECT_ALL_PROPS);
        ResultSet allContent = stmt.executeQuery();
        StringBuilder sb = new StringBuilder();
        while (allContent.next())
        {
            sb.append(allContent.getString(COL_ID_INSTANCE)).append(": ")
                .append(allContent.getString(COL_ID_KEY)).append(" = ")
                .append(allContent.getString(COL_ID_VAL)).append("\n");
        }
        allContent.close();
        stmt.close();
        connection.close();
        return sb.toString();
    }

    @SuppressWarnings("resource")
    protected ResultSet getAllProps() throws SQLException
    {
        PreparedStatement preparedStatement = getConnection().prepareStatement(SELECT_ALL_PROPS);
        add(preparedStatement); // will be closed later in GenericDbBase#tearDown()
        return preparedStatement.executeQuery();
    }

    protected void checkIfPresent(Map<String, String> origMap, String expectedInstanceName) throws SQLException
    {
        Map<String, String> map = new HashMap<>(origMap);
        ResultSet resultSet = getAllProps();
        while (resultSet.next())
        {
            String instanceName = resultSet.getString(COL_ID_INSTANCE);
            if (expectedInstanceName.equals(instanceName)) // not an assert as db may contain multiple containers
            {
                String key = resultSet.getString(COL_ID_KEY);
                String value = resultSet.getString(COL_ID_VAL);

                assertTrue("Unexpected key [" + key + "]", map.containsKey(key));
                String expectedValue = map.remove(key);
                assertEquals("Unexpected value [" + value + "]", expectedValue, value);
            }
        }

        resultSet.close();
        assertTrue("Database does not contain all expected entries", map.isEmpty());
    }

    protected void insert(String instanceName, String key, String value) throws SQLException
    {
        try (Connection con = getNewConnection())
        {
            try (PreparedStatement preparedStatement = con.prepareStatement(INSERT))
            {
                preparedStatement.setString(COL_ID_INSTANCE, instanceName);
                preparedStatement.setString(COL_ID_KEY, key);
                preparedStatement.setString(COL_ID_VAL, value);
                preparedStatement.executeUpdate();
            }
            con.commit();
        }
    }

    protected void insert(String instanceName, Map<String, String> map) throws SQLException
    {
        try (Connection con = getNewConnection())
        {
            try (PreparedStatement preparedStatement = con.prepareStatement(INSERT))
            {
                preparedStatement.setString(COL_ID_INSTANCE, instanceName);

                Set<Entry<String, String>> entrySet = map.entrySet();
                for (Entry<String, String> entry : entrySet)
                {
                    preparedStatement.setString(COL_ID_KEY, entry.getKey());
                    preparedStatement.setString(COL_ID_VAL, entry.getValue());
                    preparedStatement.executeUpdate();
                }
            }
            con.commit();
        }
    }

    protected void checkExpectedMap(Map<String, String> expectedMap, Props props)
    {
        assertEquals("Unexpected entries in PropsContainer", expectedMap.size(), props.size());

        Set<Entry<String, String>> entrySet = expectedMap.entrySet();
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
        try (Connection con = getNewConnection())
        {
            try (PreparedStatement preparedStatement = con.prepareStatement(DELETE))
            {
                preparedStatement.setString(COL_ID_INSTANCE, instanceName);
                preparedStatement.setString(COL_ID_KEY, key);
                preparedStatement.executeUpdate();
            }
            con.commit();
        }
    }

    protected void truncate() throws SQLException
    {
        try (Connection con = getNewConnection())
        {
            try (PreparedStatement stmt = con.prepareStatement(TRUNCATE_PROPS_CONTAINERS))
            {
                stmt.executeUpdate();
            }
            con.commit();
        }
    }
}
