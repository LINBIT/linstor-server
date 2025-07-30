package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.linstor.dbdrivers.SQLUtils;

import java.sql.Connection;

@Migration(
    version = "2023.11.29.09.00",
    description = "Add PEER_SLOTS column to RESOURCE_GROUPS"
)
public class Migration_2023_11_29_AddPeerSlotsToRscGrp extends LinstorMigration
{
    private static final String TBL_RSC_GRP = "RESOURCE_GROUPS";

    private static final String CLM_PEER_SLOTS = "PEER_SLOTS";
    private static final String AFTER_CLM = "DISKLESS_ON_REMAINING";

    @Override
    public void migrate(Connection conRef, DbProduct dbProduct) throws Exception
    {
        SQLUtils.executeStatement(
            conRef,
            MigrationUtils.addColumn(
                dbProduct,
                TBL_RSC_GRP,
                CLM_PEER_SLOTS,
                "SMALLINT",
                true,
                null,
                AFTER_CLM
            )
        );
    }
}
