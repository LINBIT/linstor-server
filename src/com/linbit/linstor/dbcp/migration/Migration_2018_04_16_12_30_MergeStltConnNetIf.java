package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.dbdrivers.DatabaseDriverInfo;
import com.linbit.linstor.dbdrivers.GenericDbDriver;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

@Migration(
    version = "2018.04.16.12.30",
    description = "Merging Satellite_Connections into Nodes_Net_Interfaces"
)
public class Migration_2018_04_16_12_30_MergeStltConnNetIf extends LinstorMigration
{
    private static final String TBL_NET_IF = "LINSTOR.NODE_NET_INTERFACES";
    private static final String NODE_NAME = "NODE_NAME";
    private static final String NET_NAME = "NODE_NET_NAME";

    private static final String OLD_TBL_SC =  "LINSTOR.SATELLITE_CONNECTIONS";
    private static final String OLD_SC_UUID = "UUID";
    private static final String OLD_SC_NODE_NAME = "NODE_NAME";
    private static final String OLD_SC_NODE_NET_NAME = "NODE_NET_NAME";
    private static final String OLD_SC_TCP_PORT = "TCP_PORT";
    private static final String OLD_SC_INET_TYPE = "INET_TYPE";

    private static final String NEW_NI_STLT_CONN_PORT = "STLT_CONN_PORT";
    private static final String NEW_NI_STLT_CONN_ENCR_TYPE = "STLT_CONN_ENCR_TYPE";

    @Override
    public void migrate(Connection connection)
        throws Exception
    {
        if (MigrationUtils.statementFails(connection, "SELECT " + NEW_NI_STLT_CONN_PORT + " FROM " + TBL_NET_IF))
        {
            DatabaseDriverInfo databaseInfo = DatabaseDriverInfo.createDriverInfo(getDbType());
            GenericDbDriver.executeStatement(connection, databaseInfo.isolationStatement());

            alterTableNetIf(connection);
            copyData(connection);
            dropTableStltConn(connection);

            connection.commit();
        }
    }

    private void alterTableNetIf(Connection connection) throws SQLException
    {
        connection.createStatement().execute(
            "ALTER TABLE " + TBL_NET_IF + " ADD " + NEW_NI_STLT_CONN_PORT + " SMALLINT" // nullable
        );
        connection.createStatement().execute(
            "ALTER TABLE " + TBL_NET_IF + " ADD " + NEW_NI_STLT_CONN_ENCR_TYPE + " VARCHAR(5)" // nullable
        );
    }

    @SuppressWarnings("checkstyle:magicnumber")
    private void copyData(Connection connection) throws SQLException
    {
        String query =
            "SELECT " + OLD_SC_UUID + ", " + OLD_SC_NODE_NAME + ", " + OLD_SC_NODE_NET_NAME + ", " +
             OLD_SC_TCP_PORT + ", " + OLD_SC_INET_TYPE + " " +
            " FROM " + OLD_TBL_SC;
        String update =
            " UPDATE " + TBL_NET_IF +
            " SET "   + NEW_NI_STLT_CONN_PORT + " = ? " +
            " WHERE " + NODE_NAME      + " = ? AND " +
            "       " + NET_NAME       + " = ?";

        PreparedStatement updateStmt = connection.prepareStatement(update);
        ResultSet resultSet = connection.createStatement().executeQuery(query);
        while (resultSet.next())
        {
            updateStmt.setInt(1, resultSet.getInt(OLD_SC_TCP_PORT));
            updateStmt.setString(2, OLD_SC_NODE_NAME);
            updateStmt.setString(3, OLD_SC_NODE_NET_NAME);
            updateStmt.executeQuery();
        }
        resultSet.close();
        updateStmt.close();
    }

    private void dropTableStltConn(Connection connection) throws SQLException
    {
        String dropOldSc =
            "DROP TABLE " + OLD_TBL_SC;
        connection.createStatement().execute(dropOldSc);
    }
}
