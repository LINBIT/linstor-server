package com.linbit.linstor.dbdrivers;

import com.linbit.ImplementationError;

public interface DatabaseDriverInfo
{
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

    String isolationStatement();

    String prepareInit(String initSQL);
}
