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
    private static final String POOL_NAME = "POOL_NAME";
    private static final String NODE_NAME = "NODE_NAME";

    private static final String TBL_OP = "SEC_OBJECT_PROTECTION";
    private static final String OP_OBJECT_PATH = "OBJECT_PATH";
    private static final String OP_CREATOR = "CREATOR_IDENTITY_NAME";
    private static final String OP_OWNER = "OWNER_ROLE_NAME";
    private static final String OP_SEC_TYPE_NAME = "SECURITY_TYPE_NAME";

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
                " SET " + NEW_SP_FREE_SPACE_MGR_DSP_NAME + " = CONCAT('SYSTEM:', " + POOL_NAME + "), " +
                          NEW_SP_FREE_SPACE_MGR_NAME     + " = CONCAT('SYSTEM:', " + POOL_NAME + ")" +
                " WHERE " + NEW_SP_FREE_SPACE_MGR_DSP_NAME + " = 'SYSTEM:'"
            );

            // copy the protection from the node to the protection for the free space manager
            stmt.executeUpdate(
                "INSERT INTO " + TBL_OP + " " +
                    "SELECT " +
                        "CONCAT('/freespacemgrs/', sp." + NEW_SP_FREE_SPACE_MGR_NAME + ") " + OP_OBJECT_PATH + ", " +
                        "prot." + OP_CREATOR + " " + OP_CREATOR + ", " +
                        "prot." + OP_OWNER + " " + OP_OWNER + ", " +
                        "prot." + OP_SEC_TYPE_NAME + " " + OP_SEC_TYPE_NAME + " " +
                    "FROM " + TBL_STOR_POOL + " sp " +
                    "JOIN " + TBL_OP + " prot " +
                        "ON CONCAT('/nodes/', sp." + NODE_NAME + ") = prot." + OP_OBJECT_PATH
            );

            stmt.close();
        }
    }
}
