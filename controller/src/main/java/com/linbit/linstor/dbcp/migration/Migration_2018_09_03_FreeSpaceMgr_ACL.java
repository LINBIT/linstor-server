package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo;
import java.sql.Connection;
import java.sql.Statement;

import static com.linbit.linstor.DatabaseInfo.DbProduct.MYSQL;
import static com.linbit.linstor.DatabaseInfo.DbProduct.MARIADB;

@SuppressWarnings("checkstyle:typename")
@Migration(
    version = "2018.09.03.14.30",
    description = "Fix FreeSpaceMgr SEC_ACL_MAP entries."
)
public class Migration_2018_09_03_FreeSpaceMgr_ACL extends LinstorMigration
{
    @Override
    public void migrate(Connection connection, DatabaseInfo.DbProduct dbProduct) throws Exception
    {
        Statement stmt = connection.createStatement();
        if (dbProduct == MYSQL || dbProduct == MARIADB)
        {
            stmt.executeUpdate(
                "INSERT INTO SEC_ACL_MAP SELECT CONCAT('/freespacemgrs/', sp.FREE_SPACE_MGR_NAME), acl_map.ROLE_NAME, " +
                "acl_map.ACCESS_TYPE FROM NODE_STOR_POOL sp JOIN SEC_ACL_MAP acl_map ON " +
                "CONCAT('/nodes/', sp.NODE_NAME) = acl_map.OBJECT_PATH AND " +
                "CONCAT('/freespacemgrs/', sp.FREE_SPACE_MGR_NAME) NOT IN (SELECT object_path FROM SEC_ACL_MAP)"
            );
        }
        else
        {
            stmt.executeUpdate(
                "INSERT INTO SEC_ACL_MAP SELECT '/freespacemgrs/' || sp.FREE_SPACE_MGR_NAME, acl_map.ROLE_NAME, " +
                "acl_map.ACCESS_TYPE FROM NODE_STOR_POOL sp JOIN SEC_ACL_MAP acl_map ON " +
                "'/nodes/' || sp.NODE_NAME = acl_map.OBJECT_PATH AND " +
                "'/freespacemgrs/' || sp.FREE_SPACE_MGR_NAME NOT IN (SELECT object_path FROM SEC_ACL_MAP)"
            );
        }

        stmt.executeUpdate(
            "UPDATE SEC_OBJECT_PROTECTION SET OWNER_ROLE_NAME='SYSADM', SECURITY_TYPE_NAME='SHARED' " +
            "WHERE OBJECT_PATH='/sys/controller/freeSpaceMgrMap'"
        );

        stmt.close();
    }
}
