package com.linbit.linstor;

import com.linbit.ImplementationError;


public class DatabaseInfo
{
    public enum DbProduct
    {
        UNKNOWN,
        H2,
        DERBY,
        DB2,
        DB2_I,
        DB2_Z,
        POSTGRESQL,
        ORACLE_RDBMS,
        MSFT_SQLSERVER,
        MYSQL,
        MARIADB,
        INFORMIX,
        ASE,
        ETCD;

        public String displayName()
        {
            String dspName;
            switch (this)
            {
                case UNKNOWN:
                    dspName = "unknown";
                    break;
                case H2:
                    dspName = "H2";
                    break;
                case DERBY:
                    dspName = "Apache Derby";
                    break;
                case DB2:
                    dspName = "DB2/LUW";
                    break;
                case DB2_I:
                    dspName = "DB2/System i";
                    break;
                case DB2_Z:
                    dspName = "DB2/System z";
                    break;
                case POSTGRESQL:
                    dspName = "PostgreSQL";
                    break;
                case ORACLE_RDBMS:
                    dspName = "Oracle RDBMS";
                    break;
                case MSFT_SQLSERVER:
                    dspName = "Microsoft SQLServer";
                    break;
                case MYSQL:
                    dspName = "Oracle MySQL";
                    break;
                case MARIADB:
                    dspName = "MariaDB";
                    break;
                case INFORMIX:
                    dspName = "IBM Informix";
                    break;
                case ASE:
                    dspName = "SAP Adaptive Server Enterprise (Sybase)";
                    break;
                case ETCD:
                    dspName = "Non-SQL / etcd";
                    break;
                default:
                    throw new ImplementationError(
                        "Missing case statement for enum " + name() + " (" + ordinal() + ") in class " +
                        getClass().getCanonicalName()
                    );
            }
            return dspName;
        }

        public String dbType()
        {
            String dspName;
            switch (this)
            {
                case UNKNOWN:
                    dspName = "unknown";
                    break;
                case H2:
                    dspName = "h2";
                    break;
                case DERBY:
                    dspName = "derby";
                    break;
                case DB2:
                    dspName = "db2";
                    break;
                case DB2_I:
                    dspName = "db2/i";
                    break;
                case DB2_Z:
                    dspName = "db2/z";
                    break;
                case POSTGRESQL:
                    dspName = "postgresql";
                    break;
                case ORACLE_RDBMS:
                    dspName = "oracle";
                    break;
                case MSFT_SQLSERVER:
                    dspName = "mssql";
                    break;
                case MYSQL:
                    dspName = "mysql";
                    break;
                case MARIADB:
                    dspName = "mariadb";
                    break;
                case INFORMIX:
                    dspName = "informix";
                    break;
                case ASE:
                    dspName = "sybase";
                    break;
                case ETCD:
                    dspName = "etcd";
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

    public static final int[] H2_MIN_VERSION = {1, 2};
    public static final int[] DERBY_MIN_VERSION = {10, 11};
    public static final int[] DB2_MIN_VERSION = {9, 7};
    public static final int[] POSTGRES_MIN_VERSION = {9, 0};
    public static final int[] MYSQL_MIN_VERSION = {5, 1};
    public static final int[] MARIADB_MIN_VERSION = {5, 1};
    public static final int[] INFORMIX_MIN_VERSION = {12, 10};

    private static final String ID_H2       = "H2";
    private static final String ID_DB2      = "DB2";
    private static final String ID_PGSQL    = "POSTGRESQL";
    private static final String ID_MYSQL    = "MYSQL";
    private static final String ID_ETCD     = "ETCD";

    private static final String SUB_ID_MARIADB = "MARIADB";

    public static DbProduct getDbProduct(String databaseProductName, String databaseProductVersion)
    {
        DbProduct dbProd = DbProduct.UNKNOWN;
        String dbUpperName = databaseProductName.toUpperCase();
        if (dbUpperName.equals(ID_H2))
        {
            dbProd = DbProduct.H2;
        }
        else
        if (dbUpperName.equals(ID_DB2))
        {
            dbProd = DbProduct.DB2;
        }
        else
        if (dbUpperName.startsWith(ID_DB2))
        {
            int splitIdx = dbUpperName.indexOf('/');
            if (splitIdx != -1)
            {
                String db2Id = dbUpperName.substring(0, splitIdx);
                if (db2Id.equals(ID_DB2))
                {
                    dbProd = DbProduct.DB2;
                }
            }
        }
        else
        if (dbUpperName.equals(ID_PGSQL))
        {
            dbProd = DbProduct.POSTGRESQL;
        }
        else
        if (dbUpperName.equals(ID_MYSQL))
        {
            String dbVsn = databaseProductVersion;
            dbVsn = dbVsn.toUpperCase();
            if (dbVsn.contains(SUB_ID_MARIADB))
            {
                dbProd = DbProduct.MARIADB;
            }
            else
            {
                dbProd = DbProduct.MYSQL;
            }
        }
        else
        if (dbUpperName.equals(SUB_ID_MARIADB))
        {
            dbProd = DbProduct.MARIADB;
        }
        else
        if (dbUpperName.equals(ID_ETCD))
        {
            dbProd = DbProduct.ETCD;
        }
        return dbProd;
    }

    private DatabaseInfo()
    {
    }
}
