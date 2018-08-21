package com.linbit.linstor.dbcp.migration;

import java.sql.Connection;
import java.sql.Statement;

@Migration(
    version = "2018.08.20.17.00",
    description = "Extending StorPool table with FreeSpaceMgr"
)
public class Migration_2018_08_20_17_00_FreeSpaceMgr extends LinstorMigration
{
    private static final String TBL_STOR_POOL = "NODE_STOR_POOL";
    private static final String NEW_SP_FREE_SPACE_MGR_NAME = "FREE_SPACE_MGR_NAME";
    private static final String NEW_SP_FREE_SPACE_MGR_DSP_NAME = "FREE_SPACE_MGR_DSP_NAME";
    private static final String OLD_SP_NAME = "POOL_NAME";

    @Override
    public void migrate(Connection connection) throws Exception
    {
        if (!MigrationUtils.columnExists(connection, TBL_STOR_POOL, NEW_SP_FREE_SPACE_MGR_NAME))
        {
            Statement stmt = connection.createStatement();
            stmt.executeUpdate(
                "ALTER TABLE " + TBL_STOR_POOL +
                " ADD " + NEW_SP_FREE_SPACE_MGR_NAME + " VARCHAR(55) NOT NULL" +
                " DEFAULT ('SYSTEM:')"
            // 55 = FreeSpaceName.MaxLen + "SYSTEM:".len
            );
            stmt.executeUpdate(
                "ALTER TABLE " + TBL_STOR_POOL +
                " ADD " + NEW_SP_FREE_SPACE_MGR_DSP_NAME + " VARCHAR(55) NOT NULL" +
                " DEFAULT ('SYSTEM:')"
            // 55 = FreeSpaceName.MaxLen + "SYSTEM:".len
            );
            stmt.executeUpdate(
                "ALTER TABLE " + TBL_STOR_POOL +
                " ADD CONSTRAINT CHK_SP_FREE_MGR CHECK (" +
                    NEW_SP_FREE_SPACE_MGR_NAME + " IS NULL AND " + NEW_SP_FREE_SPACE_MGR_DSP_NAME + " IS NULL OR " +
                    NEW_SP_FREE_SPACE_MGR_NAME + " = UPPER(" + NEW_SP_FREE_SPACE_MGR_DSP_NAME + "))"
            );

            stmt.executeUpdate(
                "UPDATE " + TBL_STOR_POOL +
                " SET " + NEW_SP_FREE_SPACE_MGR_DSP_NAME + " = CONCAT('SYSTEM:', " + OLD_SP_NAME + "), " +
                          NEW_SP_FREE_SPACE_MGR_NAME     + " = CONCAT('SYSTEM:', " + OLD_SP_NAME + ")" +
                " WHERE " + NEW_SP_FREE_SPACE_MGR_DSP_NAME + " = 'SYSTEM:'"
            );
            stmt.close();
        }
    }
}