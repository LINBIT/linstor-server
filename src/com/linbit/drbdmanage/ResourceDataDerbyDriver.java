package com.linbit.drbdmanage;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.TransactionMgr;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbdmanage.core.DrbdManage;
import com.linbit.drbdmanage.dbdrivers.PrimaryKey;
import com.linbit.drbdmanage.dbdrivers.derby.DerbyConstants;
import com.linbit.drbdmanage.dbdrivers.interfaces.ResourceDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.ResourceDefinitionDataDatabaseDriver;
import com.linbit.drbdmanage.logging.ErrorReporter;
import com.linbit.drbdmanage.propscon.SerialGenerator;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.ObjectProtection;
import com.linbit.drbdmanage.security.ObjectProtectionDatabaseDriver;
import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;
import com.linbit.utils.UuidUtils;

public class ResourceDataDerbyDriver implements ResourceDataDatabaseDriver
{
    private static final String TBL_RES = DerbyConstants.TBL_NODE_RESOURCE;

    private static final String RES_UUID = DerbyConstants.UUID;
    private static final String RES_NODE_NAME = DerbyConstants.NODE_NAME;
    private static final String RES_NAME = DerbyConstants.RESOURCE_NAME;
    private static final String RES_NODE_ID = DerbyConstants.NODE_ID;
    private static final String RES_FLAGS = DerbyConstants.RESOURCE_FLAGS;

    private static final String RES_SELECT =
        " SELECT " + RES_UUID + ", " + RES_NODE_NAME + ", " + RES_NAME + ", " + RES_NODE_ID + ", " + RES_FLAGS +
        " FROM " + TBL_RES +
        " WHERE " + RES_NODE_NAME + " = ? AND " +
        RES_NAME      + " = ?";
    private static final String RES_SELECT_BY_NODE =
        " SELECT " + RES_UUID + ", " + RES_NODE_NAME + ", " + RES_NAME + ", " + RES_NODE_ID + ", " + RES_FLAGS +
        " FROM " + TBL_RES +
        " WHERE " + RES_NODE_NAME + " = ?";
    private static final String RES_SELECT_BY_RES_DFN =
        " SELECT " + RES_UUID + ", " + RES_NODE_NAME + ", " + RES_NAME + ", " + RES_NODE_ID + ", " + RES_FLAGS +
        " FROM " + TBL_RES +
        " WHERE " + RES_NAME + " = ?";

    private static final String RES_INSERT =
        " INSERT INTO " + TBL_RES + " VALUES (?, ?, ?, ?, ?)";
    private static final String RES_DELETE =
        " DELETE FROM " + TBL_RES +
        " WHERE " + RES_NODE_NAME + " = ? AND " +
                    RES_NAME      + " = ?";
    private static final String RES_UPDATE_FLAG =
        " UPDATE " + TBL_RES +
        " SET " + RES_FLAGS + " = ? " +
        " WHERE " + RES_NODE_NAME + " = ? AND " +
                    RES_NAME      + " = ?";

    private static final Hashtable<PrimaryKey, ResourceData> RES_CACHE = new Hashtable<>();

    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;
    private final FlagDriver flagDriver;


    public ResourceDataDerbyDriver(AccessContext accCtx, ErrorReporter errorReporterRef)
    {
        dbCtx = accCtx;
        errorReporter = errorReporterRef;

        flagDriver = new FlagDriver();
    }

    @Override
    public void create(ResourceData res, TransactionMgr transMgr) throws SQLException
    {
        create(dbCtx, res, transMgr);
    }

    private static void create(AccessContext accCtx, ResourceData res, TransactionMgr transMgr) throws SQLException
    {
        try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(RES_INSERT))
        {
            stmt.setBytes(1, UuidUtils.asByteArray(res.getUuid()));
            stmt.setString(2, res.getAssignedNode().getName().value);
            stmt.setString(3, res.getDefinition().getName().value);
            stmt.setInt(4, res.getNodeId().value);
            stmt.setLong(5, res.getStateFlags().getFlagsBits(accCtx));
            stmt.executeUpdate();

            cache(res);
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            throw new ImplementationError(
                "Database's access context is not permitted to access resource.stateflags",
                accessDeniedExc
            );
        }
    }

    public static void ensureResExists(AccessContext accCtx, ResourceData res, TransactionMgr transMgr)
        throws SQLException
    {
        try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(RES_SELECT))
        {
            stmt.setString(1, res.getAssignedNode().getName().value);
            stmt.setString(2, res.getDefinition().getName().value);

            ResultSet resultSet = stmt.executeQuery();
            if (resultSet.next())
            {
                if (cacheGet(res) != res)
                {
                    resultSet.close();
                    throw new ImplementationError("Two different ResourceData share the same primary key", null);
                }
            }
            else
            {
                create(accCtx, res, transMgr);
            }
            resultSet.close();
        }
    }

    @Override
    public ResourceData load(
        Node node,
        ResourceName resourceName,
        SerialGenerator serialGen,
        TransactionMgr transMgr
    )
        throws SQLException
    {
        PreparedStatement stmt = transMgr.dbCon.prepareStatement(RES_SELECT);
        stmt.setString(1, node.getName().value);
        stmt.setString(2, resourceName.value);
        ResultSet resultSet = stmt.executeQuery();

        List<ResourceData> list = load(resultSet, dbCtx, node, serialGen, transMgr);
        resultSet.close();
        stmt.close();

        ResourceData ret = null;
        if (!list.isEmpty())
        {
            ret = list.get(0);
        }
        else
        {
            if (cacheGet(node, resourceName) != null)
            {
                // list is empty -> no entry in database
                // resCache knows the pk, so it was loaded or created by the db...

                // XXX: user deleted db entry during runtime - throw exception?
                // or just remove the item from the cache + detach item from parent (if needed) + warn the user?
            }
        }
        return ret;
    }


    public static List<ResourceData> loadResourceDataByResourceDefinition(
        ResourceDefinitionData resDfn,
        SerialGenerator serialGen,
        TransactionMgr transMgr,
        AccessContext accCtx
    )
        throws SQLException
    {
        PreparedStatement stmt = transMgr.dbCon.prepareStatement(RES_SELECT_BY_RES_DFN);
        stmt.setString(1, resDfn.getName().value);
        ResultSet resultSet = stmt.executeQuery();

        List<ResourceData> resList = load(resultSet, accCtx, null, serialGen, transMgr);

        resultSet.close();
        stmt.close();
        return resList;
    }

    public static List<ResourceData> loadResourceData(AccessContext dbCtx, NodeData node, SerialGenerator serialGen, TransactionMgr transMgr)
        throws SQLException
    {
        PreparedStatement stmt = transMgr.dbCon.prepareStatement(RES_SELECT_BY_NODE);
        stmt.setString(1, node.getName().value);
        ResultSet resultSet = stmt.executeQuery();

        List<ResourceData> ret = load(resultSet, dbCtx, node, serialGen, transMgr);
        resultSet.close();
        stmt.close();

        return ret;
    }

    private static List<ResourceData> load(
        ResultSet resultSet,
        AccessContext dbCtx,
        Node globalNode,
        SerialGenerator serialGen,
        TransactionMgr transMgr
    )
        throws SQLException
    {
        List<ResourceData> resList = new ArrayList<>();
        while (resultSet.next())
        {
            try
            {
                Node node;
                if (globalNode != null)
                {
                    node = globalNode;
                }
                else
                {
                    node = DrbdManage.getNodeDataDatabaseDriver().load(
                        new NodeName(resultSet.getString(RES_NODE_NAME)),
                        serialGen,
                        transMgr
                    );
                }

                ResourceData resData = cacheGet(resultSet);
                if (resData == null)
                {
                    ResourceName resName = new ResourceName(resultSet.getString(RES_NAME));
                    ResourceDefinitionDataDatabaseDriver resDfnDriver = DrbdManage.getResourceDefinitionDataDatabaseDriver();
                    ResourceDefinition resDfn = resDfnDriver.load(resName, serialGen, transMgr);

                    ObjectProtectionDatabaseDriver objProtDriver = DrbdManage.getObjectProtectionDatabaseDriver();
                    ObjectProtection objProt = objProtDriver.loadObjectProtection(
                        ObjectProtection.buildPath(resName),
                        transMgr
                    );

                    NodeId nodeId = new NodeId(resultSet.getInt(RES_NODE_ID));
                    resData = new ResourceData(
                        UuidUtils.asUuid(resultSet.getBytes(RES_UUID)),
                        objProt,
                        resDfn,
                        node,
                        nodeId,
                        resultSet.getLong(RES_FLAGS),
                        serialGen,
                        transMgr
                    );
                    if (!cache(resData))
                    {
                        resData = cacheGet(resData); // looks strange but it works :)
                    }
                    else
                    {
                        // restore volumes
                        List<VolumeData> volList = VolumeDataDerbyDriver.loadAllVolumesByResource(resData, transMgr, serialGen, dbCtx);
                        for (VolumeData volData : volList)
                        {
                            resData.setVolume(dbCtx, volData);
                        }
                    }
                }
                resList.add(resData);
            }
            catch (InvalidNameException invalidNameExc)
            {
                throw new DrbdSqlRuntimeException(
                    "A resource or node name in the table " + TBL_RES + " has been modified in the database to an illegal string.",
                    invalidNameExc
                );
            }
            catch (ValueOutOfRangeException valueOutOfRangeExc)
            {
                throw new DrbdSqlRuntimeException(
                    "A node id in the table " + TBL_RES + " has been modified in the database to an illegal value.",
                    valueOutOfRangeExc
                );
            }
            catch (AccessDeniedException accessDeniedExc)
            {
                throw new ImplementationError(
                    "Database's access context has no permission to get storPoolDefinition",
                    accessDeniedExc
                );
            }
        }
        return resList;
    }

    @Override
    public void delete(ResourceData resource, TransactionMgr transMgr) throws SQLException
    {
        PreparedStatement stmt = transMgr.dbCon.prepareStatement(RES_DELETE);

        stmt.setString(1, resource.getAssignedNode().getName().value);
        stmt.setString(2, resource.getDefinition().getName().value);

        stmt.executeUpdate();
        stmt.close();

        cacheRemove(resource.getAssignedNode().getName(), resource.getDefinition().getName());
    }

    @Override
    public StateFlagsPersistence<ResourceData> getStateFlagPersistence()
    {
        return flagDriver;
    }

    private synchronized static boolean cache(ResourceData res)
    {
        boolean ret = false;
        if (res != null)
        {
            PrimaryKey pk = getPk(res);
            boolean contains = RES_CACHE.containsKey(pk);
            if (!contains)
            {
                RES_CACHE.put(pk, res);
                ret = true;
            }
        }
        return ret;
    }

    private static ResourceData cacheGet(ResourceData value)
    {
        ResourceData ret = null;
        if (value != null)
        {
            ret = RES_CACHE.get(getPk(value));
        }
        return ret;
    }

    private static ResourceData cacheGet(ResultSet resultSet) throws SQLException
    {
        return RES_CACHE.get(getPk(resultSet));
    }

    private static ResourceData cacheGet(Node node, ResourceName resName)
    {
        return RES_CACHE.get(getPk(node.getName(), resName));
    }

    private synchronized static void cacheRemove(NodeName nodeName, ResourceName resName)
    {
        RES_CACHE.remove(getPk(nodeName, resName));
    }

    /**
     * this method should only be called by tests or if you want a full-reload from the database
     */
    static synchronized void clearCache()
    {
        RES_CACHE.clear();
    }

    private static PrimaryKey getPk(ResourceData res)
    {
        return new PrimaryKey(
            res.getAssignedNode().getName().value,
            res.getDefinition().getName().value
        );
    }

    private static PrimaryKey getPk(ResultSet resultSet) throws SQLException
    {
        return new PrimaryKey(
            resultSet.getString(RES_NODE_NAME),
            resultSet.getString(RES_NAME)
        );
    }

    private static PrimaryKey getPk(NodeName nodeName, ResourceName resName)
    {
        return new PrimaryKey(
            nodeName.value,
            resName.value
        );
    }

    private class FlagDriver implements StateFlagsPersistence<ResourceData>
    {
        @Override
        public void persist(ResourceData resource, long flags, TransactionMgr transMgr) throws SQLException
        {
            PreparedStatement stmt = transMgr.dbCon.prepareStatement(RES_UPDATE_FLAG);

            stmt.setLong(1, flags);

            stmt.setString(2, resource.getAssignedNode().getName().value);
            stmt.setString(3, resource.getDefinition().getName().value);

            stmt.executeUpdate();
            stmt.close();
        }
    }
}
