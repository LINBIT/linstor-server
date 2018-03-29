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

    public static boolean statementFails(Connection connection, String sql)
        throws SQLException
    {
        boolean fails = false;
        try
        {
            connection.createStatement().executeQuery(sql);
        }
        catch (SQLException exc)
        {
            fails = true;
            connection.rollback();
        }

        return fails;
    }

    private MigrationUtils()
    {
    }
}
