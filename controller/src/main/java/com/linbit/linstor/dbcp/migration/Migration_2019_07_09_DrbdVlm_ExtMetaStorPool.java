package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo;
import com.linbit.linstor.dbdrivers.SQLUtils;

import java.sql.Connection;

@SuppressWarnings("checkstyle:typename")
@Migration(
    version = "2019.07.09.10.42",
    description = "Add Layer_Drbd_Volumes table"
)
/*
 * For external meta data (and layer RAID layer) we will need to support multiple
 * storage pools per volume. This means one volume might have multiple storage-layer-vlm-data,
 * each of them in different storage pools
 */
public class Migration_2019_07_09_DrbdVlm_ExtMetaStorPool extends LinstorMigration
{
    @Override
    public void migrate(Connection connection, DatabaseInfo.DbProduct dbProduct) throws Exception
    {
        if (!MigrationUtils.tableExists(connection, "LAYER_DRBD_VOLUMES"))
        {
            SQLUtils.runSql(
                connection,
                MigrationUtils.loadResource(
                    "2019_07_09_add-layer-drbd-volumes.sql"
                )
            );
        }
    }
}
