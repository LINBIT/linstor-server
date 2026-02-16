package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;

import java.sql.Connection;
import java.sql.Statement;

@SuppressWarnings("checkstyle:typename")
@Migration(
    version = "2026.02.16.09.00",
    description = "Rename ExtCmdWaitTimeout to ExtCmd/WaitTimeout"
)
public class Migration_2026_02_16_RenameExtCmdWaitTo extends LinstorMigration
{
    public static final String PROP_KEY_OLD_EXT_CMD_WAIT_TIMEOUT = "ExtCmdWaitTimeout";
    public static final String PROP_KEY_NEW_EXT_CMD_WAIT_TIMEOUT = "ExtCmd/WaitTimeout";

    private static final String TBL_PROPS_CONTAINERS = "PROPS_CONTAINERS";
    private static final String CLM_PROP_KEY = "PROP_KEY";

    @Override
    public void migrate(Connection connection, DbProduct dbProduct) throws Exception
    {
        try (Statement stmt = connection.createStatement())
        {
            stmt.executeUpdate(
                "UPDATE " + TBL_PROPS_CONTAINERS +
                    " SET " + CLM_PROP_KEY + " = '" + PROP_KEY_NEW_EXT_CMD_WAIT_TIMEOUT + "'" +
                    " WHERE " + CLM_PROP_KEY + " = '" + PROP_KEY_OLD_EXT_CMD_WAIT_TIMEOUT + "'"
            );
        }
    }
}
