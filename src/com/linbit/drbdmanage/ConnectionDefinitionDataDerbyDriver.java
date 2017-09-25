package com.linbit.drbdmanage;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.core.DrbdManage;
import com.linbit.drbdmanage.dbdrivers.DerbyDriver;
import com.linbit.drbdmanage.dbdrivers.derby.DerbyConstants;
import com.linbit.drbdmanage.dbdrivers.interfaces.ConnectionDefinitionDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.NodeDataDatabaseDriver;
import com.linbit.drbdmanage.logging.ErrorReporter;
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
        TransactionMgr transMgr
    )
        throws SQLException
    {
        errorReporter.logTrace("Loading ConnectionDefinition %s", getTraceId(resDfn, sourceNodeName, targetNodeName));

        ConnectionDefinitionData ret = null;
        try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(CON_SELECT))
        {
            stmt.setString(1, resDfn.getName().value);
            stmt.setString(2, sourceNodeName.value);
            stmt.setString(3, targetNodeName.value);

            try (ResultSet resultSet = stmt.executeQuery())
            {

                if (resultSet.next())
                {
                    ret = restoreConnectionDefinition(resultSet, resDfn, transMgr);
                    // traceLog about loaded from DB|cache in restoreConDfn method
                }
                else
                {
                    errorReporter.logWarning("ConnectionDefinition not found in DB %s", getDebugId(resDfn, sourceNodeName, targetNodeName));
                }
            }
        }
        return ret;
    }

    private ConnectionDefinitionData restoreConnectionDefinition(
        ResultSet resultSet,
        ResourceDefinition resDfn,
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
        NodeData nodeSrc = nodeDriver.load(sourceNodeName, transMgr);

        ConnectionDefinitionData conData = cacheGet(resDfn, nodeSrc, conNr);

        if (conData == null)
        {
            ObjectProtection objProt = getObjectProtection(resName, sourceNodeName, targetNodeName, transMgr);

            NodeData nodeDst = nodeDriver.load(targetNodeName, transMgr);

            conData = new ConnectionDefinitionData(uuid, objProt, resDfn, nodeSrc, nodeDst, conNr);
            errorReporter.logDebug("ConnectionDefinition loaded from DB %s", getDebugId(conData));
        }
        else
        {
            errorReporter.logDebug("ConnectionDefinition loaded from cache %s", getDebugId(conData));
        }

        return conData;
    }

    private ObjectProtection getObjectProtection(
        ResourceName resName,
        NodeName sourceNodeName,
        NodeName targetNodeName,
        TransactionMgr transMgr
    )
        throws SQLException
    {
        ObjectProtectionDatabaseDriver objProtDriver = DrbdManage.getObjectProtectionDatabaseDriver();
        ObjectProtection objProt = objProtDriver.loadObjectProtection(
            ObjectProtection.buildPath(resName, sourceNodeName, targetNodeName),
            transMgr
        );
        if (objProt == null)
        {
            throw new ImplementationError(
                "ConnectionDefinition's DB entry exists, but is missing an entry in ObjProt table! " +
                    getTraceId(resName, sourceNodeName, targetNodeName),
                null
            );
        }
        return objProt;
    }

    public List<ConnectionDefinition> loadAllConnectionsByResourceDefinition(
        ResourceDefinitionData resDfn,
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
            ConnectionDefinitionData conDfn = restoreConnectionDefinition(resultSet, resDfn, transMgr);
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

            errorReporter.logTrace("Creating ConnectionDefinition %s", getTraceId(conDfnData));

            stmt.setBytes(1, UuidUtils.asByteArray(conDfnData.getUuid()));
            stmt.setString(2, resName.value);
            stmt.setString(3, sourceNodeName.value);
            stmt.setString(4, targetNodeName.value);
            stmt.setInt(5, conDfnData.getConnectionNumber(dbCtx));

            stmt.executeUpdate();

            errorReporter.logDebug("ConnectionDefinition created s", getDebugId(conDfnData));
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

            errorReporter.logTrace("Deleting ConnectionDefinition %s", getTraceId(conDfnData));
            try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(CON_DELETE))
            {
                stmt.setString(1, resName.value);
                stmt.setString(2, sourceNodeName.value);
                stmt.setString(3, targetNodeName.value);

                stmt.executeUpdate();
            }
            errorReporter.logDebug("ConnectionDefinition deleted %s", getDebugId(conDfnData));
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

    private String getTraceId(ConnectionDefinitionData conData)
    {
        String id = null;
        try
        {
            id = getId(
                conData.getResourceDefinition(dbCtx).getName().value,
                conData.getSourceNode(dbCtx).getName().value,
                conData.getTargetNode(dbCtx).getName().value
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            DerbyDriver.handleAccessDeniedException(accDeniedExc);
        }
        return id;
    }

    private String getDebugId(ConnectionDefinitionData conData)
    {
        String id = null;
        try
        {
            id = getId(
                conData.getResourceDefinition(dbCtx).getName().displayValue,
                conData.getSourceNode(dbCtx).getName().displayValue,
                conData.getTargetNode(dbCtx).getName().displayValue
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            DerbyDriver.handleAccessDeniedException(accDeniedExc);
        }
        return id;
    }

    private String getTraceId(ResourceDefinition resDfn, NodeName sourceNodeName, NodeName targetNodeName)
    {
        return getId(
            resDfn.getName().value,
            sourceNodeName.value,
            targetNodeName.value
        );
    }

    private String getDebugId(ResourceDefinition resDfn, NodeName sourceNodeName, NodeName targetNodeName)
    {
        return getId(
            resDfn.getName().displayValue,
            sourceNodeName.displayValue,
            targetNodeName.displayValue
        );
    }

    private String getTraceId(ResourceName resName, NodeName sourceNodeName, NodeName targetNodeName)
    {
        return getId(
            resName.value,
            sourceNodeName.value,
            targetNodeName.value
        );
    }

    private String getId(String resName, String sourceName, String targetName)
    {
        return "(ResName=" + resName + " SourceNode=" + sourceName + " TargetNode=" + targetName + ")";
    }

    private class ConnectionNumberDriver implements SingleColumnDatabaseDriver<ConnectionDefinitionData, Integer>
    {
        @Override
        public synchronized void update(ConnectionDefinitionData parent, Integer newConNr, TransactionMgr transMgr)
            throws SQLException
        {
            try
            {
                errorReporter.logTrace(
                    "Updating ConnectionDefinition's ConnectionNumber from [%d] to [%d] %s",
                    parent.getConnectionNumber(dbCtx),
                    newConNr,
                    getTraceId(parent)
                );
                try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(CON_UPDATE_CON_NR))
                {
                    stmt.setInt(1, newConNr);
                    stmt.setString(2, parent.getResourceDefinition(dbCtx).getName().value);
                    stmt.setString(3, parent.getSourceNode(dbCtx).getName().value);
                    stmt.setString(4, parent.getTargetNode(dbCtx).getName().value);
                    stmt.executeUpdate();
                }
                errorReporter.logDebug(
                    "ConnectionDefinition's ConnectionNumber updated from [%d] to [%d] %s",
                    parent.getConnectionNumber(dbCtx),
                    newConNr,
                    getDebugId(parent)
                );
            }
            catch (AccessDeniedException accDeniedExc)
            {
                DerbyDriver.handleAccessDeniedException(accDeniedExc);
            }
        }
    }
}
