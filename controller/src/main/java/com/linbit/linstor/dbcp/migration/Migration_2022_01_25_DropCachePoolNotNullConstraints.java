package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.linstor.dbdrivers.SQLUtils;

import java.sql.Connection;
import java.util.Arrays;
import java.util.List;

@Migration(
    version = "2022.01.25.09.00",
    description = "Drop not-null constraints for cachepools of cache, writecache and bcache"
)
public class Migration_2022_01_25_DropCachePoolNotNullConstraints extends LinstorMigration
{
    private static final String TBL_BCACHE_VLMS = "LAYER_BCACHE_VOLUMES";
    private static final String TBL_CACHE_VLMS = "LAYER_CACHE_VOLUMES";
    private static final String TBL_WRITECACHE_VLMS = "LAYER_WRITECACHE_VOLUMES";

    private static final String NODE_NAME = "NODE_NAME";
    private static final String POOL_NAME = "POOL_NAME";
    private static final String POOL_NAME_CACHE = "POOL_NAME_CACHE";
    private static final String POOL_NAME_META = "POOL_NAME_META";

    @Override
    public void migrate(Connection connection, DbProduct dbProduct) throws Exception
    {
        List<String> dropNotNullStatements = Arrays.asList(
            MigrationUtils.dropColumnConstraintNotNull(dbProduct, TBL_BCACHE_VLMS, NODE_NAME, "VARCHAR(255)"),
            MigrationUtils.dropColumnConstraintNotNull(dbProduct, TBL_BCACHE_VLMS, POOL_NAME, "VARCHAR(48)"),

            MigrationUtils.dropColumnConstraintNotNull(dbProduct, TBL_CACHE_VLMS, NODE_NAME, "VARCHAR(255)"),
            MigrationUtils.dropColumnConstraintNotNull(dbProduct, TBL_CACHE_VLMS, POOL_NAME_CACHE, "VARCHAR(48)"),
            MigrationUtils.dropColumnConstraintNotNull(dbProduct, TBL_CACHE_VLMS, POOL_NAME_META, "VARCHAR(48)"),

            MigrationUtils.dropColumnConstraintNotNull(dbProduct, TBL_WRITECACHE_VLMS, NODE_NAME, "VARCHAR(255)"),
            MigrationUtils.dropColumnConstraintNotNull(dbProduct, TBL_WRITECACHE_VLMS, POOL_NAME, "VARCHAR(48)")
        );
        for (String sql : dropNotNullStatements)
        {
            SQLUtils.runSql(connection, sql);
        }
    }
}
