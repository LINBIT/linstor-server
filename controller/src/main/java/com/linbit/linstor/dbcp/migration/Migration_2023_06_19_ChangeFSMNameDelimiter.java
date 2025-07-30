package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

@SuppressWarnings("checkstyle:typename")
@Migration(
    version = "2023.06.19.10.00",
    description = "Change FreeSpaceMgr name delimiter"
)
public class Migration_2023_06_19_ChangeFSMNameDelimiter extends LinstorMigration
{
    private static final String TBL_OBJ_PROT = "SEC_OBJECT_PROTECTION";
    private static final String TBL_ACL = "SEC_ACL_MAP";
    private static final String TBL_SP = "NODE_STOR_POOL";

    private static final String CLM_PK_OBJ_PROT = "OBJECT_PATH";

    private static final String CLM_NODE_NAME = "NODE_NAME";
    private static final String CLM_POOL_NAME = "POOL_NAME";
    private static final String CLM_FSM_DSP_NAME = "FREE_SPACE_MGR_DSP_NAME";
    private static final String CLM_FSM_NAME = "FREE_SPACE_MGR_NAME";


    @SuppressWarnings("checkstyle:magicnumber")
    @Override
    public void migrate(Connection connection, DbProduct dbProduct) throws Exception
    {
        try (
            PreparedStatement delObjProt = connection.prepareStatement(
                "DELETE FROM " + TBL_OBJ_PROT + " WHERE " + CLM_PK_OBJ_PROT + " LIKE '%freespacemgrs%'"
            );
            PreparedStatement delAcl = connection.prepareStatement(
                "DELETE FROM " + TBL_ACL + " WHERE " + CLM_PK_OBJ_PROT + " LIKE '%freespacemgrs%'"
                );
        )
        {
            delAcl.execute();
            delObjProt.execute();
        }

        try (
            PreparedStatement selectSps = connection.prepareStatement(
                "SELECT " + CLM_NODE_NAME + ", " + CLM_POOL_NAME + ", " + CLM_FSM_DSP_NAME + ", " + CLM_FSM_NAME +
                " FROM " + TBL_SP +
                " WHERE " + CLM_FSM_NAME + " LIKE '%:%'"
            );
            PreparedStatement updateSp = connection.prepareStatement(
                "UPDATE " + TBL_SP +
                " SET " + CLM_FSM_NAME +  " = ?, " +
                          CLM_FSM_DSP_NAME + " = ?" +
                " WHERE " + CLM_NODE_NAME + " = ? AND " +
                            CLM_POOL_NAME + " = ?"
                );
        )
        {
            ResultSet rs = selectSps.executeQuery();
            while (rs.next())
            {
                updateSp.setString(1, rs.getString(CLM_FSM_NAME).replace(":", ";"));
                updateSp.setString(2, rs.getString(CLM_FSM_DSP_NAME).replace(":", ";"));
                updateSp.setString(3, rs.getString(CLM_NODE_NAME));
                updateSp.setString(4, rs.getString(CLM_POOL_NAME));

                updateSp.execute();
            }
        }
    }
}
