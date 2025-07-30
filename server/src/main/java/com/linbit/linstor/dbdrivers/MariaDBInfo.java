package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.annotation.Nullable;

import static com.linbit.linstor.dbdrivers.derby.DbConstants.DATABASE_SCHEMA_NAME;

public class MariaDBInfo implements DatabaseDriverInfo
{
    private String compatType = "mariadb";

    public MariaDBInfo(String type)
    {
        compatType = type;
    }

    @Override
    public String jdbcUrl(String dbPath)
    {
        return "jdbc:" + compatType + ":" + dbPath;
    }

    @Override
    public @Nullable String jdbcInMemoryUrl()
    {
        return null;
    }

    @Override
    public String createSchemaStatement()
    {
        return String.format("CREATE SCHEMA IF NOT EXISTS `%s`;", DATABASE_SCHEMA_NAME);
    }

    @Override
    public String createVersionTableStatement()
    {
        return DatabaseDriverInfo.CREATE_TBL_SCHEMA_HISTORY.replace("\"", "`");
    }

    @Override
    public String queryVersionsStatement()
    {
        return DatabaseDriverInfo.DB_VERSIONS_QUERY_STMT.replace("\"", "`");
    }

    @Override
    public String getDbVersionHighestRankStmt()
    {
        return DatabaseDriverInfo.DB_VERSION_HIGHEST_RANK.replace("\"", "`");
    }

    @Override
    public String versionTableInsertStatement()
    {
        return DatabaseDriverInfo.DB_VERSION_INSERT.replace("\"", "`");
    }

    @Override
    public String prepareInit(String initSQL)
    {
        return initSQL;
    }
}
