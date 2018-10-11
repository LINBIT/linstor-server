package com.linbit.linstor.dbcp.migration;

import com.google.common.io.Resources;
import com.linbit.linstor.dbdrivers.derby.DbConstants;
import com.linbit.linstor.DatabaseInfo;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MigrationUtils
{
    public static final String META_COL_TABLE_NAME = "TABLE_NAME";
    public static final String META_COL_COLUMN_NAME = "COLUMN_NAME";

    private static DatabaseInfo dbInfo = null;

    public static String loadResource(String resourceName)
        throws IOException
    {
        return Resources.toString(Resources.getResource(MigrationUtils.class, resourceName), StandardCharsets.UTF_8);
    }

    public static boolean tableExists(Connection connection, String tableName)
        throws SQLException
    {
        boolean exists = false;

        DatabaseMetaData metaData = connection.getMetaData();

        // Fetch all tables in order to do a case-insensitive search
        ResultSet res = metaData.getTables(null, DbConstants.DATABASE_SCHEMA_NAME, null, null);

        while (res.next())
        {
            String resTableName = res.getString(META_COL_TABLE_NAME);
            if (tableName.equalsIgnoreCase(resTableName))
            {
                exists = true;
            }
        }

        return exists;
    }

    public static boolean columnExists(Connection connection, String tableName, String columnName)
        throws SQLException
    {
        boolean exists = false;

        DatabaseMetaData metaData = connection.getMetaData();

        // Fetch all columns in order to do a case-insensitive search
        ResultSet res = metaData.getColumns(null, DbConstants.DATABASE_SCHEMA_NAME, null, null);

        while (res.next())
        {
            String resTableName = res.getString(META_COL_TABLE_NAME);
            String resColumnName = res.getString(META_COL_COLUMN_NAME);
            if (tableName.equalsIgnoreCase(resTableName) && columnName.equalsIgnoreCase(resColumnName))
            {
                exists = true;
            }
        }

        return exists;
    }

    public static void setDatabaseInfo(DatabaseInfo dbInfoRef)
    {
        if (dbInfo == null)
        {
            dbInfo = dbInfoRef;
        }
    }

    public static DatabaseInfo getDatabaseInfo()
    {
        return dbInfo;
    }

    private MigrationUtils()
    {
    }
}
