package com.linbit.linstor.dbdrivers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class GenericDbUtils
{
    public static int executeStatement(final Connection con, final String statement)
        throws SQLException
    {
        int ret = 0;
        try (PreparedStatement stmt = con.prepareStatement(statement))
        {
            ret = stmt.executeUpdate();
        }
        catch (SQLException throwable)
        {
            System.err.println("Error: " + statement);
            throw throwable;
        }
        return ret;
    }

    private GenericDbUtils()
    {
    }
}
