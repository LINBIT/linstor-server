// Type mismatch fix:
//     Table STOR_POOL_DEFINITIONS column POOL_NAME is type VARCHAR(48)
//     Table SNAPSHOT_VOLUMES column STOR_POOL_NAME is type VARCHAR(32)
//     and references STOR_POOL_DEFINITIONS(POOL_NAME)
// Fix: Table SNAPSHOT_VOLUMES column STOR_POOL_NAME type VARCHAR(32) -> VARCHAR(48)

package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo;
import java.sql.Connection;

import static com.linbit.linstor.DatabaseInfo.DbProduct.DB2_I;
import static com.linbit.linstor.DatabaseInfo.DbProduct.INFORMIX;
import static com.linbit.linstor.DatabaseInfo.DbProduct.ASE;
import static com.linbit.linstor.DatabaseInfo.DbProduct.MARIADB;
import static com.linbit.linstor.DatabaseInfo.DbProduct.ORACLE_RDBMS;
import static com.linbit.linstor.DatabaseInfo.DbProduct.MSFT_SQLSERVER;
import java.sql.Statement;

@Migration(
    version = "2018.11.22.10.53",
    description = "Fix snapshot volumes foreign key"
)
public class Migration_2018_11_22_SnapshotVolumes_FkFix extends LinstorMigration
{
    @Override
    public void migrate(Connection connection, DatabaseInfo.DbProduct dbProduct)
        throws Exception
    {
        String sqlCmd;
        if (dbProduct == DB2_I)
        {
            sqlCmd = "ALTER TABLE SNAPSHOT_VOLUMES ALTER COLUMN STOR_POOL_NAME SET DATA TYPE VARCHAR(48) NOT NULL";
        }
        else
        if (dbProduct == MARIADB || dbProduct == ASE)
        {
            sqlCmd = "ALTER TABLE SNAPSHOT_VOLUMES MODIFY STOR_POOL_NAME VARCHAR(48)";
        }
        else
        if (dbProduct == ORACLE_RDBMS || dbProduct == INFORMIX)
        {
            sqlCmd = "ALTER TABLE SNAPSHOT_VOLUMES MODIFY (STOR_POOL_NAME VARCHAR(48))";
        }
        else
        if (dbProduct == MSFT_SQLSERVER)
        {
            sqlCmd = "ALTER TABLE SNAPSHOT_VOLUMES ALTER COLUMN STOR_POOL_NAME VARCHAR(48) NOT NULL";
        }
        else
        {
            // H2, Derby, DB2, DB2 on System z, PostgreSQL
            sqlCmd = "ALTER TABLE SNAPSHOT_VOLUMES ALTER COLUMN STOR_POOL_NAME SET DATA TYPE VARCHAR(48)";
        }
        Statement stmt = connection.createStatement();
        stmt.executeUpdate(sqlCmd);
        stmt.close();
    }
}
