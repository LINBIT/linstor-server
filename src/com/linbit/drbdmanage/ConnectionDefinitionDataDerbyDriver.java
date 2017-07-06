package com.linbit.drbdmanage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.linbit.ImplementationError;
import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.dbdrivers.PrimaryKey;
import com.linbit.drbdmanage.dbdrivers.derby.DerbyConstants;
import com.linbit.drbdmanage.dbdrivers.interfaces.NodeDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.ResourceDefinitionDataDatabaseDriver;
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

    private static final String CON_SELECT =
        " SELECT " + CON_UUID + ", " + CON_RES_NAME + ", " +
                     CON_NODE_SRC + ", " + CON_NODE_DST +
        " FROM " + TBL_CON_DFN +
        " WHERE "+ CON_RES_NAME + " = ? AND " +
                   CON_NODE_SRC + " = ? AND " +
                   CON_NODE_DST + " = ?";

    private static final String CON_INSERT =
        " INSERT INTO " + TBL_CON_DFN +
        " VALUES (?, ?, ?, ?)";
    private static final String CON_DELETE =
        " DELETE FROM " + TBL_CON_DFN +
        " WHERE "+ CON_RES_NAME + " = ? AND " +
                   CON_NODE_SRC + " = ? AND " +
                   CON_NODE_DST + " = ?";

    private static Map<PrimaryKey, ConnectionDefinitionData> conDfnCache = new HashMap<>();

    private final AccessContext dbCtx;

    public ConnectionDefinitionDataDerbyDriver(AccessContext accCtx)
    {
        dbCtx = accCtx;
    }

    @Override
    public ConnectionDefinitionData load(
        Connection con,
        ResourceName resName,
        NodeName srcNodeName,
        NodeName dstNodeName,
        TransactionMgr transMgr,
        SerialGenerator serialGen
    )
        throws SQLException
    {
        PreparedStatement stmt = con.prepareStatement(CON_SELECT);
        stmt.setString(1, resName.value);
        stmt.setString(2, srcNodeName.value);
        stmt.setString(3, dstNodeName.value);

        ResultSet resultSet = stmt.executeQuery();

        ConnectionDefinitionData ret = cacheGet(resName, srcNodeName, dstNodeName);
        if (ret == null)
        {
            if (resultSet.next())
            {
                UUID uuid = UuidUtils.asUUID(resultSet.getBytes(CON_UUID));
                ObjectProtectionDatabaseDriver objProtDriver = DrbdManage.getObjectProtectionDatabaseDriver(
                    ObjectProtection.buildPath(resName, srcNodeName, dstNodeName)
                );
                ObjectProtection objProt = objProtDriver.loadObjectProtection(con);

                ResourceDefinitionDataDatabaseDriver resDriver = DrbdManage.getResourceDefinitionDataDatabaseDriver(
                    resName
                );
                ResourceDefinitionData resDfn = resDriver.load(con, serialGen, transMgr);

                NodeDataDatabaseDriver nodeDriver = DrbdManage.getNodeDataDatabaseDriver();
                NodeData nodeSrc = nodeDriver.load(con, srcNodeName, serialGen, transMgr);
                NodeData nodeDst = nodeDriver.load(con, dstNodeName, serialGen, transMgr);

                ret = new ConnectionDefinitionData(uuid, objProt, resDfn, nodeSrc, nodeDst);
                cache(ret, dbCtx);
            }
        }
        else
        {
            if (!resultSet.next())
            {
                // XXX: user deleted db entry during runtime - throw exception?
                // or just remove the item from the cache + node.removeRes(cachedRes) + warn the user?
            }
        }

        return ret;
    }

    @Override
    public void create(Connection con, ConnectionDefinitionData conDfnData) throws SQLException
    {
        try (PreparedStatement stmt = con.prepareStatement(CON_INSERT))
        {
            stmt.setBytes(1, UuidUtils.asByteArray(conDfnData.getUuid()));
            stmt.setString(2, conDfnData.getResourceDefinition(dbCtx).getName().value);
            stmt.setString(3, conDfnData.getSourceNode(dbCtx).getName().value);
            stmt.setString(4, conDfnData.getTargetNode(dbCtx).getName().value);

            stmt.executeUpdate();
            cache(conDfnData, dbCtx);
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            throw new ImplementationError(
                "Database's access context has no permission to access ConnectionDefinitionData",
                accessDeniedExc
            );
        }
    }

    @Override
    public void delete(Connection con, ConnectionDefinitionData conDfnData) throws SQLException
    {
        try (PreparedStatement stmt = con.prepareStatement(CON_DELETE))
        {
            stmt.setString(1, conDfnData.getResourceDefinition(dbCtx).getName().value);
            stmt.setString(2, conDfnData.getSourceNode(dbCtx).getName().value);
            stmt.setString(3, conDfnData.getTargetNode(dbCtx).getName().value);

            stmt.executeUpdate();
            cacheRemove(conDfnData, dbCtx);
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            throw new ImplementationError(
                "Database's access context has no permission to access ConnectionDefinitionData",
                accessDeniedExc
            );
        }
    }

    private static void cache(ConnectionDefinitionData con, AccessContext accCtx)
    {
        conDfnCache.put(getPk(con, accCtx), con);
    }

    private static ConnectionDefinitionData cacheGet(
        ResourceName resName,
        NodeName srcNodeName,
        NodeName dstNodeName
    )
    {
        return conDfnCache.get(new PrimaryKey(resName.value, srcNodeName.value, dstNodeName.value));
    }

    /**
     * this method should only be called by tests or if you want a full-reload from the database
     */
    static void clearCache()
    {
        conDfnCache.clear();
    }

    private static void cacheRemove(ConnectionDefinitionData con, AccessContext accCtx)
    {
        conDfnCache.remove(getPk(con, accCtx));
    }

    private static PrimaryKey getPk(ConnectionDefinitionData con, AccessContext accCtx)
    {
        try
        {
            return new PrimaryKey(
                con.getResourceDefinition(accCtx).getName().value,
                con.getSourceNode(accCtx).getName().value,
                con.getTargetNode(accCtx).getName().value
            );
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            throw new ImplementationError(
                "Database's access context has no permission to access ConnectionDefinitionData",
                accessDeniedExc
            );
        }
    }


}
