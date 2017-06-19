package com.linbit.drbdmanage.propscon;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.junit.Before;

import com.linbit.drbdmanage.dbdrivers.derby.PropsConDerbyDriver;
import com.linbit.drbdmanage.security.DerbyBase;

public class DerbyPropsConBase extends DerbyBase
{

    private static final String SELECT_ALL =
        "SELECT * FROM " + TBL_PROPS_CONTAINERS;
    private static final String INSERT =
        "INSERT INTO " + TBL_PROPS_CONTAINERS + " VALUES (?, ?, ?)";
    private static final String DELETE =
        "DELETE FROM " + TBL_PROPS_CONTAINERS +
        " WHERE " + PROPS_INSTANCE + " = ? AND" +
        "       " + PROP_KEY + " = ?";

    protected static final String DEFAULT_INSTANCE_NAME = "DEFAULT_INSTANCE";


    protected PropsConDerbyDriver dbDriver;

    public DerbyPropsConBase() throws SQLException
    {
    }

    @Override
    @Before
    public void setUp() throws SQLException
    {
        super.setUp();
        dbDriver = new PropsConDerbyDriver(DEFAULT_INSTANCE_NAME);
    }

    protected String debugGetAllContent() throws SQLException
    {
        Connection connection = getConnection();
        PreparedStatement stmt = connection.prepareStatement(SELECT_ALL);
        ResultSet allContent = stmt.executeQuery();
        StringBuilder sb = new StringBuilder();
        while (allContent.next())
        {
            sb.append(allContent.getString(1)).append(": ")
                .append(allContent.getString(2)).append(" = ")
                .append(allContent.getString(3)).append("\n");
        }
        allContent.close();
        stmt.close();
        connection.close();
        return sb.toString();
    }

    protected ResultSet getAllContent() throws SQLException
    {
        PreparedStatement preparedStatement = getConnection().prepareStatement(SELECT_ALL);
        add(preparedStatement);
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
        try (Connection con = getConnection())
        {
            try (PreparedStatement preparedStatement = con.prepareStatement(INSERT))
            {
                preparedStatement.setString(1, instanceName);
                preparedStatement.setString(2, key);
                preparedStatement.setString(3, value);
                preparedStatement.executeUpdate();
            }
            con.commit();
        }
    }

    protected void insert(String instanceName, Map<String, String> map) throws SQLException
    {
        try (Connection con = getConnection())
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
            con.commit();
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
        try (Connection con = getConnection())
        {
            try (PreparedStatement preparedStatement = con.prepareStatement(DELETE))
            {
                preparedStatement.setString(1, instanceName);
                preparedStatement.setString(2, key);
                preparedStatement.executeUpdate();
            }
            con.commit();
        }
    }

    protected void truncate() throws SQLException
    {
        try (Connection con = getConnection())
        {
            try (PreparedStatement stmt = con.prepareStatement(TRUNCATE_PROPS_CONTAINERS))
            {
                stmt.executeUpdate();
            }
            con.commit();
        }
    }
}
