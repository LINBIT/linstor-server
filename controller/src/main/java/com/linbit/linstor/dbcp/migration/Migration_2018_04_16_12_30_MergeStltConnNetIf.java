package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo;
import com.linbit.linstor.api.ApiConsts;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

@Migration(
    version = "2018.04.16.12.30",
    description = "Merging Satellite_Connections into Nodes_Net_Interfaces"
)
public class Migration_2018_04_16_12_30_MergeStltConnNetIf extends LinstorMigration
{
    private static final String TBL_NET_IF = "NODE_NET_INTERFACES";
    private static final String NODE_NAME  = "NODE_NAME";
    private static final String NET_NAME   = "NODE_NET_NAME";
    private static final String TBL_PROPS_CONTAINERS = "PROPS_CONTAINERS";
    private static final String PROPS_INSTANCE       = "PROPS_INSTANCE";
    private static final String PROP_KEY             = "PROP_KEY";
    private static final String PROP_VALUE           = "PROP_VALUE";

    private static final String OLD_TBL_SC           = "SATELLITE_CONNECTIONS";
    private static final String OLD_SC_UUID          = "UUID";
    private static final String OLD_SC_NODE_NAME     = "NODE_NAME";
    private static final String OLD_SC_NODE_NET_NAME = "NODE_NET_NAME";
    private static final String OLD_SC_TCP_PORT      = "TCP_PORT";
    private static final String OLD_SC_INET_TYPE     = "INET_TYPE";

    private static final String NEW_NI_STLT_CONN_PORT      = "STLT_CONN_PORT";
    private static final String NEW_NI_STLT_CONN_ENCR_TYPE = "STLT_CONN_ENCR_TYPE";

    @Override
    public void migrate(Connection connection, DatabaseInfo.DbProduct dbProduct)
        throws Exception
    {
        if (MigrationUtils.tableExists(connection, OLD_TBL_SC))
        {
            alterTableNetIf(connection);
            copyData(connection);
            dropTableStltConn(connection);
        }
    }

    private void alterTableNetIf(Connection connection) throws SQLException
    {
        connection.createStatement().execute(
            "ALTER TABLE " + TBL_NET_IF + " ADD COLUMN " + NEW_NI_STLT_CONN_PORT + " SMALLINT" // nullable
        );
        connection.createStatement().execute(
            "ALTER TABLE " + TBL_NET_IF + " ADD COLUMN " + NEW_NI_STLT_CONN_ENCR_TYPE + " VARCHAR(5)" // nullable
        );
    }

    @SuppressWarnings("checkstyle:magicnumber")
    private void copyData(Connection connection) throws SQLException
    {
        String query =
            "SELECT " + OLD_SC_UUID + ", " + OLD_SC_NODE_NAME + ", " + OLD_SC_NODE_NET_NAME + ", " +
             OLD_SC_TCP_PORT + ", " + OLD_SC_INET_TYPE + " " +
            " FROM " + OLD_TBL_SC;
        String updateNetIfTbl =
            " UPDATE " + TBL_NET_IF +
            " SET "   + NEW_NI_STLT_CONN_PORT      + " = ?, " +
                        NEW_NI_STLT_CONN_ENCR_TYPE + " = ? " +
            " WHERE " + NODE_NAME + " = ? AND " +
            "       " + NET_NAME  + " = ?";
        String insertNodeProps =
            " INSERT INTO " + TBL_PROPS_CONTAINERS +
            " (" + PROPS_INSTANCE + ", " + PROP_KEY + ", " + PROP_VALUE + ") " +
            " VALUES (?, ?, ?)";


        PreparedStatement netIfUpdateStmt = connection.prepareStatement(updateNetIfTbl);
        PreparedStatement insertNodePropsStmt = connection.prepareStatement(insertNodeProps);
        ResultSet resultSet = connection.createStatement().executeQuery(query);
        while (resultSet.next())
        {
            netIfUpdateStmt.setInt(1, resultSet.getInt(OLD_SC_TCP_PORT));
            netIfUpdateStmt.setString(2, resultSet.getString(OLD_SC_INET_TYPE));
            netIfUpdateStmt.setString(3, resultSet.getString(OLD_SC_NODE_NAME));
            netIfUpdateStmt.setString(4, resultSet.getString(OLD_SC_NODE_NET_NAME));
            netIfUpdateStmt.executeUpdate();

            insertNodePropsStmt.setString(1, "/NODES/" + resultSet.getString(OLD_SC_NODE_NAME));
            insertNodePropsStmt.setString(2, ApiConsts.KEY_CUR_STLT_CONN_NAME);
            insertNodePropsStmt.setString(3, resultSet.getString(OLD_SC_NODE_NET_NAME));
            insertNodePropsStmt.executeUpdate();
        }
        resultSet.close();
        netIfUpdateStmt.close();
        insertNodePropsStmt.close();
    }

    private void dropTableStltConn(Connection connection) throws SQLException
    {
        // This potentially drops various foreign key constraints from other tables
        // Fixed in migration 2019_03_15_FixConstraints
        String dropOldSc =
            "DROP TABLE " + OLD_TBL_SC;
        Statement dropTblStmt = connection.createStatement();
        dropTblStmt.executeUpdate(dropOldSc);
        dropTblStmt.close();
    }
}
