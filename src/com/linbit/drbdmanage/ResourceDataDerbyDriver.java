package com.linbit.drbdmanage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.TransactionMgr;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbdmanage.Resource.RscFlags;
import com.linbit.drbdmanage.dbdrivers.PrimaryKey;
import com.linbit.drbdmanage.dbdrivers.derby.DerbyConstants;
import com.linbit.drbdmanage.dbdrivers.interfaces.PropsConDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.ResourceDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.ResourceDefinitionDataDatabaseDriver;
import com.linbit.drbdmanage.propscon.InvalidKeyException;
import com.linbit.drbdmanage.propscon.InvalidValueException;
import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.propscon.PropsConDerbyDriver;
import com.linbit.drbdmanage.propscon.PropsContainer;
import com.linbit.drbdmanage.propscon.SerialGenerator;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.ObjectProtection;
import com.linbit.drbdmanage.security.ObjectProtectionDatabaseDriver;
import com.linbit.drbdmanage.stateflags.StateFlags;
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

    private static final String RES_SELECT_BY_NODE =
        " SELECT " + RES_UUID + ", " + RES_NODE_NAME + ", " + RES_NAME + ", " + RES_NODE_ID + ", " + RES_FLAGS +
        " FROM " + TBL_RES +
        " WHERE " + RES_NODE_NAME + " = ?";
    private static final String RES_SELECT = RES_SELECT_BY_NODE +
        " AND " + RES_NAME + " = ?";

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

    private AccessContext dbCtx;
    private ResourceName resName;
    private NodeName nodeName;
    private FlagDriver flagDriver;

    private PropsConDatabaseDriver propsDriver;

    private static Hashtable<PrimaryKey, ResourceData> resCache = new Hashtable<>();

    public ResourceDataDerbyDriver(AccessContext accCtx, NodeName nodeNameRef, ResourceName resNameRef)
    {
        dbCtx = accCtx;
        nodeName = nodeNameRef;
        resName = resNameRef;

        flagDriver = new FlagDriver();
        propsDriver = new PropsConDerbyDriver(PropsContainer.buildPath(nodeName, resNameRef));
    }

    @Override
    public void create(Connection con, ResourceData res) throws SQLException
    {
        create(con, res, dbCtx);
    }

    private static void create(Connection con, ResourceData res, AccessContext accCtx) throws SQLException
    {
        try (PreparedStatement stmt = con.prepareStatement(RES_INSERT))
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

    public static void ensureResExists(Connection con, ResourceData value, AccessContext accCtx) throws SQLException
    {
        try (PreparedStatement stmt = con.prepareStatement(RES_SELECT))
        {
            stmt.setString(1, value.getAssignedNode().getName().value);
            stmt.setString(2, value.getDefinition().getName().value);

            ResultSet resultSet = stmt.executeQuery();
            if (resultSet.next())
            {
                if (cacheGet(value) != value)
                {
                    resultSet.close();
                    throw new ImplementationError("Two different ResourceData share the same primary key", null);
                }
            }
            else
            {
                create(con, value, accCtx);
            }
            resultSet.close();
        }
    }

    @Override
    public ResourceData load(Connection con, Node node, SerialGenerator serialGen, TransactionMgr transMgr)
        throws SQLException
    {
        PreparedStatement stmt = con.prepareStatement(RES_SELECT);
        stmt.setString(1, node.getName().value);
        stmt.setString(2, resName.value);
        ResultSet resultSet = stmt.executeQuery();

        List<ResourceData> list = load(con, resultSet, dbCtx, node, serialGen, transMgr);
        resultSet.close();
        stmt.close();

        ResourceData ret = null;
        if (!list.isEmpty())
        {
            ret = list.get(0);
        }
        else
        {
            if (cacheGet(node, resName) != null)
            {
                // list is empty -> no entry in database
                // resCache knows the pk, so it was loaded or created by the db...

                // XXX: user deleted db entry during runtime - throw exception?
                // or just remove the item from the cache + node.removeRes(cachedRes) + warn the user?
            }
        }
        return ret;
    }

    @Override
    public void delete(Connection con, ResourceData res) throws SQLException
    {
        deleteRes(con, res);
    }

    public static void deleteRes(Connection con, ResourceData res) throws SQLException
    {
        PreparedStatement stmt = con.prepareStatement(RES_DELETE);

        stmt.setString(1, res.getAssignedNode().getName().value);
        stmt.setString(2, res.getDefinition().getName().value);

        stmt.executeUpdate();
        stmt.close();

        cacheRemove(res);
    }


    public static List<ResourceData> loadResourceData(Connection con, AccessContext dbCtx, NodeData node, SerialGenerator serialGen, TransactionMgr transMgr)
        throws SQLException
    {
        PreparedStatement stmt = con.prepareStatement(RES_SELECT_BY_NODE);
        stmt.setString(1, node.getName().value);
        ResultSet resultSet = stmt.executeQuery();

        List<ResourceData> ret = load(con, resultSet, dbCtx, node, serialGen, transMgr);
        resultSet.close();
        stmt.close();

        return ret;
    }

    private static List<ResourceData> load(
        Connection con,
        ResultSet resultSet,
        AccessContext dbCtx,
        Node node,
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
                ResourceData resData = cacheGet(resultSet);
                if (resData == null)
                {
                    ResourceName resName = new ResourceName(resultSet.getString(RES_NAME));
                    ResourceDefinitionDataDatabaseDriver resDfnDriver = DrbdManage.getResourceDefinitionDataDatabaseDriver(resName);
                    ResourceDefinition resDfn = resDfnDriver.load(con, serialGen, transMgr);

                    ObjectProtectionDatabaseDriver objProtDriver = DrbdManage.getObjectProtectionDatabaseDriver(
                        ObjectProtection.buildPath(resName)
                    );
                    ObjectProtection objProt = objProtDriver.loadObjectProtection(con);

                    NodeId nodeId = new NodeId(resultSet.getInt(RES_NODE_ID));
                    resData = new ResourceData(
                        UuidUtils.asUUID(resultSet.getBytes(RES_UUID)),
                        objProt,
                        resDfn,
                        node,
                        nodeId,
                        serialGen,
                        transMgr
                    );

                    // restore props
                    PropsConDatabaseDriver propDriver = DrbdManage.getPropConDatabaseDriver(PropsContainer.buildPath(node.getName(), resName));
                    Props props = resData.getProps(dbCtx);
                    Map<String, String> loadedProps = propDriver.load(con);
                    for (Entry<String, String> entry : loadedProps.entrySet())
                    {
                        try
                        {
                            props.setProp(entry.getKey(), entry.getValue());
                        }
                        catch (InvalidKeyException | InvalidValueException invalidException)
                        {
                            throw new DrbdSqlRuntimeException(
                                "Invalid property loaded from instance: " + PropsContainer.buildPath(node.getName(), resName),
                                invalidException
                            );
                        }
                    }

                    // restore flags
                    long lFlags = resultSet.getLong(RES_FLAGS);
                    StateFlags<RscFlags> stateFlags = resData.getStateFlags();
                    for (RscFlags flag : RscFlags.values())
                    {
                        if ((lFlags & flag.flagValue) == flag.flagValue)
                        {
                            stateFlags.enableFlags(dbCtx, flag);
                        }
                    }

                    // restore volumes
                    List<VolumeData> volList = VolumeDataDerbyDriver.loadAllVolumesByResource(con, resData, transMgr, serialGen, dbCtx);
                    for (VolumeData volData : volList)
                    {
                        resData.setVolume(dbCtx, volData);
                    }

                    // TODO: gh - restore connections

                    cache(resData);
                }
                resList.add(resData);
            }
            catch (InvalidNameException invalidNameExc)
            {
                throw new DrbdSqlRuntimeException(
                    "A resource name in the table " + TBL_RES + " has been modified in the database to an illegal string.",
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
                    "Database's access context is not permitted to access resource.props",
                    accessDeniedExc
                );
            }
        }
        return resList;
    }


    @Override
    public StateFlagsPersistence getStateFlagPersistence()
    {
        return flagDriver;
    }
    
    @Override
    public PropsConDatabaseDriver getPropsConDriver()
    {
        return propsDriver;
    }

    private static void cache(ResourceData res)
    {
        if (res != null)
        {
            resCache.put(getPk(res), res);
       }
    }

    private static ResourceData cacheGet(ResourceData value)
    {
        ResourceData ret = null;
        if (value != null)
        {
            ret = resCache.get(getPk(value));
        }
        return ret;
    }

    private static ResourceData cacheGet(ResultSet resultSet) throws SQLException
    {
        return resCache.get(getPk(resultSet));
    }

    private static ResourceData cacheGet(Node node, ResourceName resName)
    {
        return resCache.get(getPk(node, resName));
    }

    private static void cacheRemove(ResourceData res)
    {
        resCache.remove(getPk(res));
    }

    /**
     * this method should only be called by tests or if you want a full-reload from the database
     */
    static void clearCache()
    {
        resCache.clear();
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

    private static PrimaryKey getPk(Node node, ResourceName resName)
    {
        return new PrimaryKey(
            node.getName().value,
            resName.value
        );
    }

    private class FlagDriver implements StateFlagsPersistence
    {
        @Override
        public void persist(Connection con, long flags) throws SQLException
        {
            PreparedStatement stmt = con.prepareStatement(RES_UPDATE_FLAG);

            stmt.setLong(1, flags);

            stmt.setString(2, nodeName.value);
            stmt.setString(3, resName.value);

            stmt.executeUpdate();
            stmt.close();
        }
    }
}
