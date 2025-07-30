package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.dbdrivers.SQLUtils;

import java.sql.Connection;

@SuppressWarnings("checkstyle:typename")
@Migration(
    version = "2019.07.23.13.37",
    description = "Add resource- and volume-groups"
)
/*
 * For external meta data (and layer RAID layer) we will need to support multiple
 * storage pools per volume. This means one volume might have multiple storage-layer-vlm-data,
 * each of them in different storage pools
 */
public class Migration_2019_07_23_ResourceGroups extends LinstorMigration
{
    @Override
    public void migrate(Connection connection, DbProduct dbProduct) throws Exception
    {
        if (!MigrationUtils.tableExists(connection, "RESOURCE_GROUPS"))
        {
            String createTables = MigrationUtils.loadResource("2019_07_23_add-rsc-grp.sql");
            createTables = MigrationUtils.replaceTypesByDialect(dbProduct, createTables);
            SQLUtils.runSql(
                connection,
                createTables
            );

            SQLUtils.runSql(
                connection,
                MigrationUtils.addColumn(
                    dbProduct,
                    "RESOURCE_DEFINITIONS",
                    "RESOURCE_GROUP_NAME",
                    "VARCHAR(255)",
                    false,
                    InternalApiConsts.DEFAULT_RSC_GRP_NAME.toUpperCase(),
                    null
                )
            );
        }
    }
}
