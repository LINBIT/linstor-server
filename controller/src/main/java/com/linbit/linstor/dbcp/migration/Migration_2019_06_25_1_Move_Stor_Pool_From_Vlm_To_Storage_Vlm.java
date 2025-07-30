package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.linstor.dbdrivers.SQLUtils;

import java.sql.Connection;

@SuppressWarnings("checkstyle:typename")
@Migration(
    version = "2019.06.25.14.27",
    description = "Move vlm.pool_name reference to storage_vlm.pool_name"
)
/*
 * For external meta data (and layer RAID layer) we will need to support multiple
 * storage pools per volume. This means one volume might have multiple storage-layer-vlm-data,
 * each of them in different storage pools
 */
public class Migration_2019_06_25_1_Move_Stor_Pool_From_Vlm_To_Storage_Vlm extends LinstorMigration
{
    private static final String TBL_LSV = "LAYER_STORAGE_VOLUMES";
    private static final String NODE_NAME = "NODE_NAME";
    private static final String LSV_NODE_NAME_TYPE = "VARCHAR(255)";
    private static final String STOR_POOL_NAME = "STOR_POOL_NAME";
    private static final String LSV_STOR_POOL_NAME_TYPE = "VARCHAR(48)";

    @Override
    public void migrate(Connection connection, DbProduct dbProduct) throws Exception
    {
        if (!MigrationUtils.columnExists(connection, TBL_LSV, NODE_NAME))
        {
            SQLUtils.runSql(
                connection,
                MigrationUtils.addColumn(
                    dbProduct,
                    TBL_LSV,
                    NODE_NAME,
                    LSV_NODE_NAME_TYPE,
                    true,
                    null,
                    null
                )
            );

            SQLUtils.runSql(
                connection,
                MigrationUtils.addColumn(
                    dbProduct,
                    TBL_LSV,
                    STOR_POOL_NAME,
                    LSV_STOR_POOL_NAME_TYPE,
                    true,
                    null,
                    null
                )
            );

            SQLUtils.runSql(
                connection,
                "UPDATE LAYER_STORAGE_VOLUMES LSV SET NODE_NAME=" +
                "(" +
                    " SELECT V.NODE_NAME FROM LAYER_RESOURCE_IDS LRI, VOLUMES V " +
                    " WHERE LSV.LAYER_RESOURCE_ID = LRI.LAYER_RESOURCE_ID AND" +
                    "  V.NODE_NAME = LRI.NODE_NAME AND" +
                    "  V.RESOURCE_NAME = LRI.RESOURCE_NAME AND" +
                    "  V.VLM_NR = LSV.VLM_NR" +
                ");"
            );
            SQLUtils.runSql(
                connection,
                "UPDATE LAYER_STORAGE_VOLUMES LSV SET STOR_POOL_NAME=" +
                "(" +
                    "SELECT V.STOR_POOL_NAME FROM LAYER_RESOURCE_IDS LRI, VOLUMES V " +
                    " WHERE LSV.LAYER_RESOURCE_ID = LRI.LAYER_RESOURCE_ID AND " +
                    "  V.NODE_NAME = LRI.NODE_NAME AND " +
                    "  V.RESOURCE_NAME = LRI.RESOURCE_NAME AND " +
                    "  V.VLM_NR = LSV.VLM_NR" +
                ");"
            );

            SQLUtils.runSql(
                connection,
                MigrationUtils.addColumnConstraintNotNull(
                    dbProduct,
                    TBL_LSV,
                    NODE_NAME,
                    LSV_NODE_NAME_TYPE
                )
            );
            SQLUtils.runSql(
                connection,
                MigrationUtils.addColumnConstraintNotNull(
                    dbProduct,
                    TBL_LSV,
                    STOR_POOL_NAME,
                    LSV_STOR_POOL_NAME_TYPE
                )
            );
            SQLUtils.runSql(
                connection,
                "ALTER TABLE " + TBL_LSV + " ADD CONSTRAINT FK_LSV_SP " +
                "FOREIGN KEY (" + NODE_NAME + ", " + STOR_POOL_NAME + ") REFERENCES NODE_STOR_POOL(" +
                    NODE_NAME + ", POOL_NAME) ON DELETE CASCADE;"
            );


            SQLUtils.runSql(
                connection,
                MigrationUtils.dropColumnConstraintForeignKey(dbProduct, "VOLUMES", "FK_V_STOR_POOL_DFNS")
            );
            SQLUtils.runSql(
                connection,
                MigrationUtils.dropColumn(dbProduct, "VOLUMES", "STOR_POOL_NAME")
            );
        }
    }
}
