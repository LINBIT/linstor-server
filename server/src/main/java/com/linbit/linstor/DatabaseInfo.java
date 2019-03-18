package com.linbit.linstor;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

public interface DatabaseInfo
{
    enum DbProduct
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
        ASE
    }

    DbProduct getDbProduct(DatabaseMetaData dbMd)
        throws SQLException;
}
