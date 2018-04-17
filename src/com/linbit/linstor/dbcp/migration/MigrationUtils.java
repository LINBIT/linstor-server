package com.linbit.linstor.dbcp.migration;

import com.google.common.io.Resources;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;

public class MigrationUtils
{
    public static String loadResource(String resourceName)
        throws IOException
    {
        return Resources.toString(Resources.getResource(MigrationUtils.class, resourceName), StandardCharsets.UTF_8);
    }

    public static boolean statementSucceeds(Connection connection, String sql)
        throws SQLException
    {
        boolean succeeds = true;
        try
        {
            connection.createStatement().executeQuery(sql);
        }
        catch (SQLException exc)
        {
            succeeds = false;
        }
        finally
        {
            connection.rollback();
        }

        return succeeds;
    }

    public static boolean tableExists(Connection connection, String tableName)
        throws SQLException
    {
        return statementSucceeds(connection, "SELECT 1 FROM " + tableName);
    }

    private MigrationUtils()
    {
    }
}
