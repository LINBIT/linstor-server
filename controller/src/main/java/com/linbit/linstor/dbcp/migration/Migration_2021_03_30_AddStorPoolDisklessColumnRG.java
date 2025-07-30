package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.linstor.dbdrivers.SQLUtils;

import java.sql.Connection;

@Migration(
    version = "2021.03.30.12.00",
    description = "Add StorPoolDiskless column to autoplacer config in ResourceGroup"
)
public class Migration_2021_03_30_AddStorPoolDisklessColumnRG extends LinstorMigration
{
    private static final String TBL_RG = "RESOURCE_GROUPS";
    private static final String COL_NAME_POOL_NAME = "POOL_NAME";
    private static final String COL_NAM_NODE_NAME_LIST = "NODE_NAME_LIST";
    private static final String COL_NAM_DO_NOT_PLACE_WITH_RSC_REGEX = "DO_NOT_PLACE_WITH_RSC_REGEX";
    private static final String COL_NAM_DO_NOT_PLACE_WITH_RSC_LIST = "DO_NOT_PLACE_WITH_RSC_LIST";
    private static final String COL_NAM_REPLICAS_ON_SAME = "REPLICAS_ON_SAME";
    private static final String COL_NAM_REPLICAS_ON_DIFFERENT = "REPLICAS_ON_DIFFERENT";

    private static final String[] COL_NAMES_FOR_TYPE_CHANGE = new String[]
    {
        COL_NAME_POOL_NAME,
        COL_NAM_NODE_NAME_LIST,
        COL_NAM_DO_NOT_PLACE_WITH_RSC_REGEX,
        COL_NAM_DO_NOT_PLACE_WITH_RSC_LIST,
        COL_NAM_REPLICAS_ON_SAME,
        COL_NAM_REPLICAS_ON_DIFFERENT
    };

    private static final String COL_NAME_POOL_NAME_DISKLESS = "POOL_NAME_DISKLESS";

    @Override
    public void migrate(Connection connection, DbProduct dbProduct) throws Exception
    {
        /*
         * MariaDB (and maybe others too) only allow a maximum row sizes up to 64k.
         * The RESOURCE_GROUPS table so far has maximum of 19563 characters. As apparently all
         * chars require roughly 3 bytes (I assume BOOLEAN and INTEGER are exceptions here)
         * our current RESOURCE_GROUPS already consumes 58689 bytes.
         *
         * Adding another VARCHAR(4096) would result in 71085 bytes, which causes an error.
         *
         * Therefore, this migration changes the large 4k field from VARCHAR(4096) to TEXT type:
         * * NODE_NAME_LIST
         * * POOL_NAME
         * * POOL_NAME_DISKLESS (newly added)
         * * DO_NOT_PLACE_WITH_RSC_REGEX
         * * DO_NOT_PLACE_WITH_RSC_LIST
         *
         * additionally (for consistency) chaning from BLOB to TEXT:
         * * REPLICAS_ON_SAME
         * * REPLICAS_ON_DIFFERENT
         */
        for (String colName : COL_NAMES_FOR_TYPE_CHANGE)
        {
            SQLUtils.runSql(
                connection,
                MigrationUtils.alterColumnType(dbProduct, TBL_RG, colName, "TEXT")
            );
        }
        SQLUtils.runSql(
            connection,
            MigrationUtils.addColumn(
                dbProduct,
                TBL_RG,
                COL_NAME_POOL_NAME_DISKLESS,
                "TEXT",
                true,
                null,
                COL_NAME_POOL_NAME
            )
        );
    }
}
