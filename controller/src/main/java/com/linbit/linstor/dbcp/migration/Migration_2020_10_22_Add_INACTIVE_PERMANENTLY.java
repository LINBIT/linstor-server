package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/*
 * ETCD migration not needed.
 */
@SuppressWarnings("checkstyle:typename")
@Migration(
    version = "2020.10.22.13.42",
    description = "Add INACTIVE_PERMANENTLY flag to already INACTIVE DRBD resources"
)
public class Migration_2020_10_22_Add_INACTIVE_PERMANENTLY extends LinstorMigration
{
    private static final long INACTIVE_BIT = 1L << 10;
    private static final long INACTIVE_PERMANENTLY_BITS = INACTIVE_BIT | 1L << 12;

    private static final String SELECT_DRBD_RESOURCES_WITH_FLAGS =
        "SELECT R.NODE_NAME, R.RESOURCE_NAME, R.RESOURCE_FLAGS " +
        "FROM RESOURCES AS R, LAYER_RESOURCE_IDS AS LRI " +
        "WHERE " +
           "LRI.LAYER_RESOURCE_KIND = 'DRBD' AND " +
           "R.NODE_NAME = LRI.NODE_NAME AND " +
           "R.RESOURCE_NAME = LRI.RESOURCE_NAME";
    private static final String UPDATE_RSC_FLAGS =
        "UPDATE RESOURCES " +
        "SET RESOURCE_FLAGS = ? " +
        "WHERE NODE_NAME = ? AND RESOURCE_NAME = ?";

    @Override
    public void migrate(Connection connection, DbProduct dbProduct) throws Exception
    {
        // in 1.9.0 all resources that are inactive AND have DRBD in the stack
        // can no longer be reactivated.

        // there might be the case where a resource was deactivated but not shipped.
        // bad luck...

        try (
            PreparedStatement selectStmt = connection.prepareStatement(SELECT_DRBD_RESOURCES_WITH_FLAGS);
            ResultSet rs = selectStmt.executeQuery();
            PreparedStatement updStmt = connection.prepareStatement(UPDATE_RSC_FLAGS);
        )
        {
            while (rs.next())
            {
                long rscFlags = rs.getLong("RESOURCE_FLAGS");
                if ((rscFlags & INACTIVE_BIT) == INACTIVE_BIT)
                {
                    updStmt.setLong(1, rscFlags | INACTIVE_PERMANENTLY_BITS);
                    updStmt.setString(2, rs.getString("NODE_NAME"));
                    updStmt.setString(3, rs.getString("RESOURCE_NAME"));

                    updStmt.execute();
                }
            }
        }
    }
}
