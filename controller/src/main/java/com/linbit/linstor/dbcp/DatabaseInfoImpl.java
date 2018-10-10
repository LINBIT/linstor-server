package com.linbit.linstor.dbcp;

import com.linbit.linstor.dbcp.DatabaseInfo.DbProduct;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * Provides information about the database that the Controller is connected to
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
@Singleton
public class DatabaseInfoImpl implements DatabaseInfo
{
    private static final String ID_H2       = "H2";
    private static final String ID_DB2      = "DB2";
    private static final String ID_PGSQL    = "POSTGRESQL";
    private static final String ID_MYSQL    = "MYSQL";

    private static final String SUB_ID_MARIADB = "MARIADB";

    @Inject
    public DatabaseInfoImpl()
    {
    }

    public DbProduct getDbProduct(DatabaseMetaData dbMd)
        throws SQLException
    {
        DbProduct dbProd = DbProduct.UNKNOWN;
        String dbName = dbMd.getDatabaseProductName();
        String dbUpperName = dbName.toUpperCase();
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
            int splitIdx = dbUpperName.indexOf("/");
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
            String dbVsn = dbMd.getDatabaseProductVersion();
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
        return dbProd;
    }
}
