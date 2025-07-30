package com.linbit.linstor.dbdrivers;

import com.linbit.ImplementationError;

import static com.linbit.linstor.dbdrivers.derby.DbConstants.DATABASE_SCHEMA_NAME;

public interface DatabaseDriverInfo
{
    /*
     * DO NOT remove the escaped " in the SQL statement since some database dialects automatically toUpper cases names
     * in SQL statements and other dialects don't. This is the way FlyWay set up the table, and we have to keep using it
     * with the given lower- or upper-case.
     */
    String CREATE_TBL_SCHEMA_HISTORY = "CREATE TABLE IF NOT EXISTS \"FLYWAY_SCHEMA_HISTORY\"(\n" +
        "    \"installed_rank\" INT NOT NULL PRIMARY KEY,\n" +
        "    \"version\" VARCHAR(50),\n" +
        "    \"description\" VARCHAR(200) NOT NULL,\n" +
        "    \"type\" VARCHAR(20) NOT NULL,\n" +
        "    \"script\" VARCHAR(1000) NOT NULL,\n" +
        "    \"checksum\" INT,\n" +
        "    \"installed_by\" VARCHAR(100) NOT NULL,\n" +
        "    \"installed_on\" TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,\n" +
        "    \"execution_time\" INT NOT NULL,\n" +
        "    \"success\" BOOLEAN NOT NULL)";
    String DB_VERSIONS_QUERY_STMT = "SELECT \"installed_rank\", \"version\" FROM \"FLYWAY_SCHEMA_HISTORY\" " +
        "WHERE \"version\" IS NOT NULL ORDER BY \"version\"";
    String DB_VERSION_HIGHEST_RANK = "SELECT \"installed_rank\" FROM \"FLYWAY_SCHEMA_HISTORY\" " +
        "WHERE \"version\" IS NOT NULL ORDER BY \"version\" DESC LIMIT 1";
    String DB_VERSION_INSERT = "INSERT INTO \"FLYWAY_SCHEMA_HISTORY\" " +
        "VALUES( ?, ?, ?, 'JDBC', ?, null, 'LINSTOR', CURRENT_TIMESTAMP, ?, TRUE)";

    enum DatabaseType
    {
        SQL,
        ETCD,
        K8S_CRD;

        public String displayName()
        {
            String dspName;
            switch (this)
            {
                case SQL:
                    dspName = "SQL";
                    break;
                case ETCD:
                    dspName = "etcd";
                    break;
                case K8S_CRD:
                    dspName = "Kubernetes-CRD";
                    break;
                default:
                    throw new ImplementationError(
                        "Missing case statement for enum " + name() + " (" + ordinal() + ") in class " +
                        getClass().getCanonicalName()
                    );
            }
            return dspName;
        }
    }

    static DatabaseDriverInfo createDriverInfo(final String dbType)
    {
        DatabaseDriverInfo dbdriver = null;
        switch (dbType)
        {
            case "h2":
                dbdriver = new H2DatabaseInfo();
                break;
            case "derby":
                dbdriver = new DerbyDatabaseInfo();
                break;
            case "db2":
                dbdriver = new Db2DatabaseInfo();
                break;
            case "postgresql":
                dbdriver = new PostgresqlDatabaseInfo();
                break;
            case "mysql":
                // fall-through
            case "mariadb":
                dbdriver = new MariaDBInfo(dbType);
                break;
            default:
                throw new RuntimeException(String.format("Database type '%s' not implemented.", dbType));
        }

        return dbdriver;
    }

    String jdbcUrl(String dbPath);
    String jdbcInMemoryUrl();

    default String createSchemaStatement()
    {
        return String.format("CREATE SCHEMA IF NOT EXISTS \"%s\";", DATABASE_SCHEMA_NAME);
    }

    default String createVersionTableStatement()
    {
        return CREATE_TBL_SCHEMA_HISTORY;
    }

    default String queryVersionsStatement()
    {
        return DB_VERSIONS_QUERY_STMT;
    }

    default String getDbVersionHighestRankStmt()
    {
        return DB_VERSION_HIGHEST_RANK;
    }

    default String versionTableInsertStatement()
    {
        return DB_VERSION_INSERT;
    }

    String prepareInit(String initSQL);
}
