package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.dbdrivers.DatabaseDriverInfo;
import com.linbit.linstor.dbdrivers.DerbyDriver;

import java.sql.Connection;

@Migration(
    version = "2018.03.27.14.01",
    description = "Initialize the database"
)
public class Migration_2018_03_27_14_01_Init extends LinstorMigration
{
    @Override
    public void migrate(Connection connection)
        throws Exception
    {
        if (MigrationUtils.statementFails(connection, "SELECT 1 FROM SEC_CONFIGURATION"))
        {
            String sql = MigrationUtils.loadResource("2018_03_27_14_01_init-db.sql");

            DatabaseDriverInfo databaseInfo = DatabaseDriverInfo.createDriverInfo(getDbType());
            DerbyDriver.executeStatement(connection, databaseInfo.isolationStatement());
            DerbyDriver.runSql(connection, databaseInfo.prepareInit(sql));
        }
    }
}
