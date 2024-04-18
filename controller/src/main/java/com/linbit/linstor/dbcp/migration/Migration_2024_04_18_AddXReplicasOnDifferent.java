package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.linstor.dbdrivers.SQLUtils;

import java.sql.Connection;

@Migration(
    version = "2024.04.18.09.00",
    description = "Add X_REPLICAS_ON_DIFFERENT column to RESOURCE_GROUPS"
)
public class Migration_2024_04_18_AddXReplicasOnDifferent extends LinstorMigration
{
    private static final String TBL_RSC_GRP = "RESOURCE_GROUPS";

    private static final String CLM_X_REPLICAS_ON_DIFFERENT = "X_REPLICAS_ON_DIFFERENT";
    private static final String AFTER_CLM = "REPLICAS_ON_DIFFERENT";

    @Override
    protected void migrate(Connection conRef, DbProduct dbProduct) throws Exception
    {
        SQLUtils.executeStatement(
            conRef,
            MigrationUtils.addColumn(
                dbProduct,
                TBL_RSC_GRP,
                CLM_X_REPLICAS_ON_DIFFERENT,
                "TEXT",
                true,
                null,
                AFTER_CLM
            )
        );
    }
}
