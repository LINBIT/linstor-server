package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo;
import com.linbit.linstor.dbdrivers.DatabaseDriverInfo;
import com.linbit.linstor.dbdrivers.SQLUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.UUID;

@Migration(
    // version is kept to not cause confusions in existing flyway-tables (which tracks applied migrations)
    version = "2018.03.27.14.01",
    description = "Consolidated initialial database"
)
public class Migration_2018_03_27_14_01_ConsolidatedInit extends LinstorMigration
{
    @SuppressWarnings("checkstyle:magicnumber")
    @Override
    public void migrate(Connection connection, DatabaseInfo.DbProduct dbProduct)
        throws Exception
    {
        if (!MigrationUtils.tableExists(connection, "SEC_CONFIGURATION"))
        {
            // the name of the SQL file does not necessarily needs to follow the same date as the file itself, but just
            // keep the migration and .sql file consistent / easier to find/map to each other, we also keep the same
            // date.
            String sql = MigrationUtils.loadResource("2018_03_27_14_01_consolidated-init-db.sql");
            sql = MigrationUtils.replaceTypesByDialect(dbProduct, sql);

            DatabaseDriverInfo databaseInfo = DatabaseDriverInfo.createDriverInfo(dbProduct.dbType());
            SQLUtils.runSql(connection, databaseInfo.prepareInit(sql));

            // add dynamic / deliberately random data:
            try (
                PreparedStatement ps = connection.prepareStatement(
                    "INSERT INTO PROPS_CONTAINERS(PROPS_INSTANCE, PROP_KEY, PROP_VALUE) VALUES(?, ?, ?)"
                ))
            {
                UUID randomClusterId = UUID.randomUUID();
                ps.setString(1, "/CTRLCFG");
                ps.setString(2, "Cluster/LocalID");
                ps.setString(3, randomClusterId.toString());
                ps.execute();

                ps.setString(1, "STLTCFG");
                ps.execute();
            }
        }
    }
}
