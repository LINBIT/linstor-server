package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbdrivers.SQLUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Objects;

@SuppressWarnings("checkstyle:typename")
@Migration(
    version = "2025.05.27.08.00",
    description = "Move TCP Port number allocation from RscDfn to Rsc"
)
public class Migration_2025_05_27_MoveTcpPortsToNodes extends LinstorMigration
{
    private static final String TBL_LAYER_DRBD_RSC_DFN = "LAYER_DRBD_RESOURCE_DEFINITIONS";
    private static final String TBL_LAYER_DRBD_RSC = "LAYER_DRBD_RESOURCES";
    private static final String TBL_LRI = "LAYER_RESOURCE_IDS";
    private static final String TBL_RSC_CONS = "RESOURCE_CONNECTIONS";

    private static final String CLM_LRI = "LAYER_RESOURCE_ID";
    private static final String CLM_RSC_NAME = "RESOURCE_NAME";
    private static final String CLM_RSC_NAME_SUFFIX = "RESOURCE_NAME_SUFFIX";
    private static final String CLM_LAYER_RESOURCE_SUFFIX = "LAYER_RESOURCE_SUFFIX";
    private static final String CLM_NODE_ID = "NODE_ID";
    private static final String CLM_NODE_NAME_SRC = "NODE_NAME_SRC";
    private static final String CLM_NODE_NAME_DST = "NODE_NAME_DST";
    private static final String CLM_SNAPSHOT_NAME = "SNAPSHOT_NAME";

    private static final String CLM_TO_DROP_TCP_PORT = "TCP_PORT";

    private static final String CLM_RENAME_FROM_TCP_PORT = "TCP_PORT";
    private static final String CLM_RENAME_TO_TCP_PORT = "TCP_PORT_SRC";

    private static final String CLM_NEW_TCP_PORT_LIST = "TCP_PORT_LIST";
    private static final String CLM_NEW_TCP_PORT_LIST_SQL_TYPE = "VARCHAR(4096)";
    private static final String CLM_NEW_TCP_PORT_LIST_DFLT_VAL = "[]";

    private static final String CLM_NEW_TCP_PORT_DST = "TCP_PORT_DST";
    private static final String CLM_NEW_TCP_PORT_TARGET_SQL_TYPE = "INTEGER";

    private static final String SELECT_DRBD_RSC_DFN_PORTS = "SELECT " + CLM_RSC_NAME + ", " + CLM_RSC_NAME_SUFFIX +
        ", " + CLM_TO_DROP_TCP_PORT + " FROM " + TBL_LAYER_DRBD_RSC_DFN;
    private static final String SELECT_LAYER_RSC_IDS = "SELECT " +
        CLM_LRI + ", " + CLM_RSC_NAME + ", " + CLM_LAYER_RESOURCE_SUFFIX + ", " + CLM_SNAPSHOT_NAME +
        " FROM " + TBL_LRI +
        " WHERE LAYER_RESOURCE_KIND = 'DRBD'";
    private static final String SELECT_RSC_CONNS = "SELECT " + CLM_NODE_NAME_SRC + ", " + CLM_NODE_NAME_DST + ", " +
        CLM_RSC_NAME + ", " + CLM_SNAPSHOT_NAME + ", " + CLM_RENAME_TO_TCP_PORT +
        " FROM " + TBL_RSC_CONS;

    private static final String UPDATE_DRBD_RSC = "UPDATE " + TBL_LAYER_DRBD_RSC +
        " SET " + CLM_NEW_TCP_PORT_LIST + " = ? " +
        " WHERE " + CLM_LRI + " = ?";
    private static final int UPD_TCP_PORT_LIST_IDX = 1;
    private static final int UPD_LRI_IDX = 2;

    private static final String UPDATE_RSC_CONN_PORTS = "UPDATE " + TBL_RSC_CONS +
        " SET " + CLM_NEW_TCP_PORT_DST + " = ? " +
        " WHERE " + CLM_NODE_NAME_SRC + " = ? AND " +
                    CLM_NODE_NAME_DST + " = ? AND " +
                    CLM_RSC_NAME + " = ? AND " +
                    CLM_SNAPSHOT_NAME + " = ''";
    private static final int UPD_RSC_CONN_PORT_DST_PORT_IDX = 1;
    private static final int UPD_RSC_CONN_PORT_DST_NODE_NAME_SRC_IDX = 2;
    private static final int UPD_RSC_CONN_PORT_DST_NODE_NAME_DST_IDX = 3;
    private static final int UPD_RSC_CONN_PORT_DST_RSC_NAME_IDX = 4;

    @Override
    protected void migrate(Connection conRef, DbProduct dbProduct) throws Exception
    {
        HashMap<RscDfnKey, Integer> tcpPortMap = getTcpPortMap(conRef);

        SQLUtils.executeStatement(
            conRef,
            MigrationUtils.addColumn(
                dbProduct,
                TBL_LAYER_DRBD_RSC,
                CLM_NEW_TCP_PORT_LIST,
                CLM_NEW_TCP_PORT_LIST_SQL_TYPE,
                false,
                CLM_NEW_TCP_PORT_LIST_DFLT_VAL,
                CLM_NODE_ID
            )
        );

        SQLUtils.executeStatement(
            conRef,
            MigrationUtils.addColumn(
                dbProduct,
                TBL_RSC_CONS,
                CLM_NEW_TCP_PORT_DST,
                CLM_NEW_TCP_PORT_TARGET_SQL_TYPE,
                true,
                null,
                CLM_RENAME_FROM_TCP_PORT
            )
        );

        SQLUtils.executeStatement(
            conRef,
            MigrationUtils.renameColumn(
                dbProduct,
                TBL_RSC_CONS,
                CLM_RENAME_FROM_TCP_PORT,
                CLM_RENAME_TO_TCP_PORT
            )
        );

        setTcpPortsOnLayerDrbdResourceTbl(conRef, tcpPortMap);

        // copy the TCP_PORT_SRC into TCP_PORT_DST (if set) for resource connections
        copyTcpPortSrcToTcpPortDstOnRscConTbl(conRef);

        // SQLUtils.executeStatement(
        // conRef,
        // MigrationUtils.dropColumn(
        // dbProduct,
        // TBL_LAYER_DRBD_RSC_DFN,
        // CLM_TO_DROP_TCP_PORT
        // )
        // );
    }

    private HashMap<RscDfnKey, Integer> getTcpPortMap(Connection conRef) throws SQLException
    {
        HashMap<RscDfnKey, Integer> tcpPortMap = new HashMap<>();
        try (
            PreparedStatement prepStmt = conRef.prepareStatement(SELECT_DRBD_RSC_DFN_PORTS);
            ResultSet selectResultSet = prepStmt.executeQuery();)
        {
            while (selectResultSet.next())
            {
                String rscName = selectResultSet.getString(CLM_RSC_NAME);
                String rscNameSuffix = selectResultSet.getString(CLM_RSC_NAME_SUFFIX);
                int tcpPort = selectResultSet.getInt(CLM_TO_DROP_TCP_PORT);
                if (!selectResultSet.wasNull())
                {
                    tcpPortMap.put(new RscDfnKey(rscName, rscNameSuffix), tcpPort);
                }
            }
        }
        return tcpPortMap;
    }

    private void setTcpPortsOnLayerDrbdResourceTbl(Connection conRef, HashMap<RscDfnKey, Integer> tcpPortMap)
        throws SQLException
    {
        try (
            PreparedStatement selectPrepStmt = conRef.prepareStatement(SELECT_LAYER_RSC_IDS);
            ResultSet selectResultSet = selectPrepStmt.executeQuery();
            PreparedStatement updatePrepStmt = conRef.prepareStatement(UPDATE_DRBD_RSC);
        )
        {
            while (selectResultSet.next())
            {
                int lri = selectResultSet.getInt(CLM_LRI);
                String rscName = selectResultSet.getString(CLM_RSC_NAME);
                String rscNameSuffix = selectResultSet.getString(CLM_LAYER_RESOURCE_SUFFIX);
                String snapName = selectResultSet.getString(CLM_SNAPSHOT_NAME);

                if (!snapName.isEmpty())
                {
                    updatePrepStmt.setString(UPD_TCP_PORT_LIST_IDX, "[-1]");
                    updatePrepStmt.setInt(UPD_LRI_IDX, lri);
                    updatePrepStmt.addBatch();
                }
                else
                {
                    @Nullable Integer tcpPort = tcpPortMap.get(new RscDfnKey(rscName, rscNameSuffix));
                    if (tcpPort != null)
                    {
                        updatePrepStmt.setString(UPD_TCP_PORT_LIST_IDX, "[" + tcpPort + "]");
                        updatePrepStmt.setInt(UPD_LRI_IDX, lri);
                        updatePrepStmt.addBatch();
                    }
                }
            }
            updatePrepStmt.executeBatch();
        }
    }

    private void copyTcpPortSrcToTcpPortDstOnRscConTbl(Connection conRef) throws SQLException
    {
        try (
            PreparedStatement selectPrepStmt = conRef.prepareStatement(SELECT_RSC_CONNS);
            ResultSet selectResultSet = selectPrepStmt.executeQuery();
            PreparedStatement updatePrepStmt = conRef.prepareStatement(UPDATE_RSC_CONN_PORTS);
        )
        {
            while (selectResultSet.next())
            {
                int tcpPortSrc = selectResultSet.getInt(CLM_RENAME_TO_TCP_PORT);
                if (!selectResultSet.wasNull())
                {
                    String snapName = selectResultSet.getString(CLM_SNAPSHOT_NAME);
                    if (selectResultSet.wasNull() || snapName.isEmpty())
                    {
                        String nodeNameSrc = selectResultSet.getString(CLM_NODE_NAME_SRC);
                        String nodeNameDst = selectResultSet.getString(CLM_NODE_NAME_DST);
                        String rscName = selectResultSet.getString(CLM_RSC_NAME);

                        updatePrepStmt.setInt(UPD_RSC_CONN_PORT_DST_PORT_IDX, tcpPortSrc);
                        updatePrepStmt.setString(UPD_RSC_CONN_PORT_DST_NODE_NAME_SRC_IDX, nodeNameSrc);
                        updatePrepStmt.setString(UPD_RSC_CONN_PORT_DST_NODE_NAME_DST_IDX, nodeNameDst);
                        updatePrepStmt.setString(UPD_RSC_CONN_PORT_DST_RSC_NAME_IDX, rscName);

                        updatePrepStmt.addBatch();
                    }
                }
            }
            updatePrepStmt.executeBatch();
        }
    }

    public static class RscDfnKey
    {
        public final String rscName;
        public final String rscNameSuffix;

        public RscDfnKey(String rscNameRef, String rscNameSuffixRef)
        {
            rscName = rscNameRef;
            rscNameSuffix = rscNameSuffixRef;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(rscName, rscNameSuffix);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
            {
                return true;
            }
            if (!(obj instanceof RscDfnKey))
            {
                return false;
            }
            RscDfnKey other = (RscDfnKey) obj;
            return Objects.equals(rscName, other.rscName) &&
                Objects.equals(rscNameSuffix, other.rscNameSuffix);
        }
    }
}
