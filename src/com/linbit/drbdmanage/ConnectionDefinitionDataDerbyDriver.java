package com.linbit.drbdmanage;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.linbit.InvalidNameException;
import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.core.DrbdManage;
import com.linbit.drbdmanage.dbdrivers.DerbyDriver;
import com.linbit.drbdmanage.dbdrivers.derby.DerbyConstants;
import com.linbit.drbdmanage.dbdrivers.interfaces.ConnectionDefinitionDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.NodeDataDatabaseDriver;
import com.linbit.drbdmanage.logging.ErrorReporter;
import com.linbit.drbdmanage.propscon.SerialGenerator;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.ObjectProtection;
import com.linbit.drbdmanage.security.ObjectProtectionDatabaseDriver;
import com.linbit.utils.UuidUtils;

public class ConnectionDefinitionDataDerbyDriver implements ConnectionDefinitionDataDatabaseDriver
{
    private static final String TBL_CON_DFN = DerbyConstants.TBL_CONNECTION_DEFINITIONS;

    private static final String CON_UUID = DerbyConstants.UUID;
    private static final String CON_RES_NAME = DerbyConstants.RESOURCE_NAME;
    private static final String CON_NODE_SRC = DerbyConstants.NODE_NAME_SRC;
    private static final String CON_NODE_DST = DerbyConstants.NODE_NAME_DST;
    private static final String CON_NR = DerbyConstants.CON_NR;

    private static final String CON_SELECT =
        " SELECT " + CON_UUID + ", " + CON_RES_NAME + ", " +
                     CON_NODE_SRC + ", " + CON_NODE_DST + ", " + CON_NR +
        " FROM " + TBL_CON_DFN +
        " WHERE "+ CON_RES_NAME + " = ? AND " +
                   CON_NODE_SRC + " = ? AND " +
                   CON_NODE_DST + " = ?";
    private static final String CON_SELECT_BY_RES_DFN =
        " SELECT " + CON_UUID + ", " + CON_RES_NAME + ", " +
                     CON_NODE_SRC + ", " + CON_NODE_DST + ", " + CON_NR +
        " FROM " + TBL_CON_DFN +
        " WHERE "+ CON_RES_NAME + " = ?";

    private static final String CON_INSERT =
        " INSERT INTO " + TBL_CON_DFN +
        " VALUES (?, ?, ?, ?, ?)";

    private static final String CON_DELETE =
        " DELETE FROM " + TBL_CON_DFN +
        " WHERE "+ CON_RES_NAME + " = ? AND " +
                   CON_NODE_SRC + " = ? AND " +
                   CON_NODE_DST + " = ?";

    private static final String CON_UPDATE_CON_NR =
        " UPDATE " + TBL_CON_DFN +
        " SET " + CON_NR + " = ?" +
        " WHERE " + CON_RES_NAME + " = ? AND " +
                    CON_NODE_SRC + " = ? AND " +
                    CON_NODE_DST + " = ?";

    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;

    private final SingleColumnDatabaseDriver<ConnectionDefinitionData, Integer> conNrDriver;

    public ConnectionDefinitionDataDerbyDriver(AccessContext accCtx, ErrorReporter errorReporterRef)
    {
        dbCtx = accCtx;
        errorReporter = errorReporterRef;

        conNrDriver = new ConnectionNumberDriver();
    }

    @Override
    public ConnectionDefinitionData load(
        ResourceDefinition resDfn,
        NodeName sourceNodeName,
        NodeName targetNodeName,
        SerialGenerator serialGen,
        TransactionMgr transMgr
    )
        throws SQLException
    {
        errorReporter.logDebug(
            "Loading ConnectionDefinition (Res=%s, SrcNode=%s, DstNode=%s)",
            resDfn.getName().value,
            sourceNodeName.value,
            targetNodeName.value
        );

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(CON_SELECT);
        stmt.setString(1, resDfn.getName().value);
        stmt.setString(2, sourceNodeName.value);
        stmt.setString(3, targetNodeName.value);

        ResultSet resultSet = stmt.executeQuery();

        ConnectionDefinitionData ret = null;
        if (resultSet.next())
        {
            ret = restoreConnectionDefinition(resultSet, resDfn, serialGen, transMgr);
            errorReporter.logTrace(
                "ConnectionDefinition loaded successfully (Res=%s, SrcNode=%s, DstNode=%s)",
                resDfn.getName().displayValue,
                sourceNodeName.displayValue,
                targetNodeName.displayValue
                );
        }
        else
        {
            errorReporter.logWarning(
                String.format(
                    "The specified connection definition (resName=%s, srcNode=%s, dstNode=%s) was not found in the database",
                     resDfn.getName().displayValue,
                     sourceNodeName.displayValue,
                     targetNodeName.displayValue
                )
            );
        }
        resultSet.close();
        stmt.close();

        return ret;
    }

    private ConnectionDefinitionData restoreConnectionDefinition(
        ResultSet resultSet,
        ResourceDefinition resDfn,
        SerialGenerator serialGen,
        TransactionMgr transMgr
    )
        throws SQLException
    {
        UUID uuid = UuidUtils.asUuid(resultSet.getBytes(CON_UUID));
        ResourceName resName;
        NodeName sourceNodeName;
        NodeName targetNodeName;
        int conNr = resultSet.getInt(CON_NR);

        try
        {
            resName = new ResourceName(resultSet.getString(CON_RES_NAME));
            sourceNodeName = new NodeName(resultSet.getString(CON_NODE_SRC));
            targetNodeName = new NodeName(resultSet.getString(CON_NODE_DST));
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw new DrbdSqlRuntimeException(
                "A resource or node name in the table " + TBL_CON_DFN +
                    " has been modified in the database to an illegal string.",
                invalidNameExc
            );
        }

        NodeDataDatabaseDriver nodeDriver = DrbdManage.getNodeDataDatabaseDriver();
        NodeData nodeSrc = nodeDriver.load(sourceNodeName, serialGen, transMgr);

        ConnectionDefinitionData conData = cacheGet(resDfn, nodeSrc, conNr);

        if (conData == null)
        {
            ObjectProtectionDatabaseDriver objProtDriver = DrbdManage.getObjectProtectionDatabaseDriver();
            ObjectProtection objProt = objProtDriver.loadObjectProtection(
                ObjectProtection.buildPath(resName, sourceNodeName, targetNodeName),
                transMgr
            );

            NodeData nodeDst = nodeDriver.load(targetNodeName, serialGen, transMgr);

            conData = new ConnectionDefinitionData(uuid, objProt, resDfn, nodeSrc, nodeDst, conNr);
        }
        return conData;
    }

    public List<ConnectionDefinition> loadAllConnectionsByResourceDefinition(
        ResourceDefinitionData resDfn,
        SerialGenerator serialGen,
        TransactionMgr transMgr
    )
        throws SQLException
    {
        PreparedStatement stmt = transMgr.dbCon.prepareStatement(CON_SELECT_BY_RES_DFN);
        stmt.setString(1, resDfn.getName().value);
        ResultSet resultSet = stmt.executeQuery();
        List<ConnectionDefinition> connections = new ArrayList<>();
        while (resultSet.next())
        {
            ConnectionDefinitionData conDfn = restoreConnectionDefinition(resultSet, resDfn, serialGen, transMgr);
            connections.add(conDfn);
        }
        resultSet.close();
        stmt.close();
        return connections;
    }

    @Override
    public void create(ConnectionDefinitionData conDfnData, TransactionMgr transMgr) throws SQLException
    {
        try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(CON_INSERT))
        {

            final ResourceName resName = conDfnData.getResourceDefinition(dbCtx).getName();
            final NodeName sourceNodeName = conDfnData.getSourceNode(dbCtx).getName();
            final NodeName targetNodeName = conDfnData.getTargetNode(dbCtx).getName();

            errorReporter.logDebug(
                "Creating connection definition (Res=%s, SrcNode=%s, DstNode=%s)",
                resName.value,
                sourceNodeName.value,
                targetNodeName.value
            );

            stmt.setBytes(1, UuidUtils.asByteArray(conDfnData.getUuid()));
            stmt.setString(2, resName.value);
            stmt.setString(3, sourceNodeName.value);
            stmt.setString(4, targetNodeName.value);
            stmt.setInt(5, conDfnData.getConnectionNumber(dbCtx));

            stmt.executeUpdate();

            errorReporter.logTrace(
                "Connection definition created (Res=%s, SrcNode=%s, DstNode=%s)",
                resName.displayValue,
                sourceNodeName.displayValue,
                targetNodeName.displayValue
            );
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            DerbyDriver.handleAccessDeniedException(accessDeniedExc);
        }
    }

    @Override
    public void delete(ConnectionDefinitionData conDfnData, TransactionMgr transMgr) throws SQLException
    {
        try
        {
            final ResourceName resName = conDfnData.getResourceDefinition(dbCtx).getName();
            final NodeName sourceNodeName = conDfnData.getSourceNode(dbCtx).getName();
            final NodeName targetNodeName = conDfnData.getTargetNode(dbCtx).getName();

            errorReporter.logDebug(
                "Deleting connection definition (Res=%s, SrcNode=%s, DstNode=%s)",
                resName.value,
                sourceNodeName.value,
                targetNodeName.value
            );
            try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(CON_DELETE))
            {
                stmt.setString(1, resName.value);
                stmt.setString(2, sourceNodeName.value);
                stmt.setString(3, targetNodeName.value);

                stmt.executeUpdate();
            }
            errorReporter.logTrace(
                "Connection definition deleted (Res=%s, SrcNode=%s, DstNode=%s)",
                resName.displayValue,
                sourceNodeName.displayValue,
                targetNodeName.displayValue
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            DerbyDriver.handleAccessDeniedException(accDeniedExc);
        }
    }

    @Override
    public SingleColumnDatabaseDriver<ConnectionDefinitionData, Integer> getConnectionNumberDriver()
    {
        return conNrDriver;
    }

    private ConnectionDefinitionData cacheGet(
        ResourceDefinition resDfn,
        NodeData node,
        int conNr
    )
    {
        ConnectionDefinitionData ret = null;
        try
        {
            ret = (ConnectionDefinitionData) resDfn.getConnectionDfn(
                dbCtx,
                node.getName(),
                conNr
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            DerbyDriver.handleAccessDeniedException(accDeniedExc);
        }
        return ret;
    }

    private class ConnectionNumberDriver implements SingleColumnDatabaseDriver<ConnectionDefinitionData, Integer>
    {
        @Override
        public synchronized void update(
            ConnectionDefinitionData parent,
            Integer newConNr,
            TransactionMgr transMgr
        )
            throws SQLException
        {
            try
            {
                final ResourceName resName = parent.getResourceDefinition(dbCtx).getName();
                final NodeName sourceNodeName = parent.getSourceNode(dbCtx).getName();
                final NodeName targetNodeName = parent.getTargetNode(dbCtx).getName();

                errorReporter.logDebug(
                    "Updating connection definition's connection number (Res=%s, SrcNode=%s, DstNode=%s)",
                    resName.value,
                    sourceNodeName.value,
                    targetNodeName.value
                );
                PreparedStatement stmt = transMgr.dbCon.prepareStatement(CON_UPDATE_CON_NR);
                stmt.setInt(1, newConNr);
                stmt.setString(2, resName.value);
                stmt.setString(3, sourceNodeName.value);
                stmt.setString(4, targetNodeName.value);
                stmt.executeUpdate();
                stmt.close();

                errorReporter.logTrace(
                    "Connection definition's connection number updated (Res=%s, SrcNode=%s, DstNode=%s)",
                    resName.displayValue,
                    sourceNodeName.displayValue,
                    targetNodeName.displayValue
                );
            }
            catch (AccessDeniedException accessDeniedExc)
            {
                DerbyDriver.handleAccessDeniedException(accessDeniedExc);
            }
        }
    }
}
