package com.linbit.drbdmanage;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.UUID;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ObjectDatabaseDriver;
import com.linbit.drbdmanage.NetInterface.NetInterfaceType;
import com.linbit.drbdmanage.dbdrivers.PrimaryKey;
import com.linbit.drbdmanage.dbdrivers.UpdateOnlyDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.derby.DerbyConstants;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.ObjectProtection;
import com.linbit.drbdmanage.security.ObjectProtectionDatabaseDriver;
import com.linbit.utils.UuidUtils;

public class NetInterfaceDataDerbyDriver implements NetInterfaceDataDatabaseDriver
{
    private static final String TBL_NODE_NET = DerbyConstants.TBL_NODE_NET_INTERFACES;

    private static final String NODE_NAME = DerbyConstants.NODE_NAME;

    private static final String NET_UUID = DerbyConstants.UUID;
    private static final String NET_NAME = DerbyConstants.NODE_NET_NAME;
    private static final String NET_DSP_NAME = DerbyConstants.NODE_NET_DSP_NAME;
    private static final String INET_ADDRESS = DerbyConstants.INET_ADDRESS;
    private static final String INET_TYPE = DerbyConstants.INET_TRANSPORT_TYPE;

    private static final String NNI_SELECT_BY_NODE =
        " SELECT "  + NET_UUID + ", " + NODE_NAME + ", " + NET_NAME + ", " + NET_DSP_NAME + ", " +
                      INET_ADDRESS + ", " + INET_TYPE +
        " FROM " + TBL_NODE_NET +
        " WHERE " + NODE_NAME + " = ?";
    private static final String NNI_SELECT_BY_NODE_AND_NET =
        " SELECT "  + NET_UUID + ", " + NODE_NAME + ", " + NET_NAME + ", " + NET_DSP_NAME + ", " +
                      INET_ADDRESS + ", " + INET_TYPE +
        " FROM " + TBL_NODE_NET +
        " WHERE " + NODE_NAME + " = ? AND " +
                    NET_NAME  + " = ?";

    private static final String NNI_INSERT =
        " INSERT INTO " + TBL_NODE_NET +
        " VALUES (?, ?, ?, ?, ?, ?)";
    private static final String NNI_DELETE =
        " DELETE FROM " + TBL_NODE_NET +
        " WHERE " + NODE_NAME + " = ? AND " +
        "       " + NET_NAME  + " = ?";
    private static final String NNI_UPDATE_ADR =
        " UPDATE " + TBL_NODE_NET +
        " SET "   + INET_ADDRESS  + " = ? " +
        " WHERE " + NODE_NAME     + " = ? AND " +
        "       " + NET_NAME      + " = ?";
    private static final String NNI_UPDATE_TYPE =
        " UPDATE " + TBL_NODE_NET +
        " SET "   + INET_TYPE + " = ? " +
        " WHERE " + NODE_NAME + " = ? AND " +
        "       " + NET_NAME  + " = ?";

    private final Node node;

    private final ObjectDatabaseDriver<InetAddress> netIfAddressDriver;
    private final ObjectDatabaseDriver<NetInterfaceType> netIfTypeDriver;

    private final AccessContext dbCtx;

    private static Hashtable<PrimaryKey, NetInterfaceData> niCache = new Hashtable<>();

    private NetInterfaceName netName;
    private boolean netNameLoaded = false;

    public NetInterfaceDataDerbyDriver(AccessContext ctx, Node nodeRef, NetInterfaceName nameRef)
    {
        dbCtx = ctx;
        node = nodeRef;
        netName = nameRef;

        netIfAddressDriver = new NodeNetInterfaceAddressDriver();
        netIfTypeDriver = new NodeNetInterfaceTypeDriver();
    }

    @Override
    public NetInterfaceData load(Connection con)
        throws SQLException, AccessDeniedException
    {
        PreparedStatement stmt = con.prepareStatement(NNI_SELECT_BY_NODE_AND_NET);
        stmt.setString(1, node.getName().value);
        stmt.setString(2, netName.value);
        ResultSet resultSet = stmt.executeQuery();
        NetInterfaceData netIfData = cacheGet(node, netName);
        if (netIfData == null)
        {
            if (resultSet.next())
            {
                if (!netNameLoaded)
                {
                    try
                    {
                        netName = new NetInterfaceName(resultSet.getString(NET_DSP_NAME));
                        netNameLoaded = true;
                    }
                    catch (InvalidNameException invalidNameExc)
                    {
                        resultSet.close();
                        stmt.close();
                        throw new ImplementationError(
                            "The display name of a valid NetInterfaceName could not be restored",
                            invalidNameExc
                        );
                    }
                }

                netIfData = restoreInstance(con, node, netName, resultSet);
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
        resultSet.close();
        stmt.close();

        return netIfData;
    }

    @Override
    public void create(Connection con, NetInterfaceData netInterfaceData) throws SQLException
    {
        PreparedStatement stmt = con.prepareStatement(NNI_INSERT);

        InetAddress inetAddress = getAddress(netInterfaceData);
        NetInterfaceType type = getNetInterfaceType(netInterfaceData);

        stmt.setBytes(1, UuidUtils.asByteArray(netInterfaceData.getUuid()));
        stmt.setString(2, node.getName().value);
        stmt.setString(3, netInterfaceData.getName().value);
        stmt.setString(4, netInterfaceData.getName().displayValue);
        stmt.setString(5, inetAddress.getHostAddress());
        stmt.setString(6, type.name());

        stmt.executeUpdate();
        stmt.close();

        cache(netInterfaceData);
    }

    public void ensureEntryExists(Connection con, NetInterfaceData netIfData) throws SQLException
    {
        PreparedStatement stmt = con.prepareStatement(NNI_SELECT_BY_NODE_AND_NET);
        stmt.setString(1, node.getName().value);
        stmt.setString(2, netName.value);
        ResultSet resultSet = stmt.executeQuery();

        NetInterfaceData niCached = cacheGet(netIfData.getNode(), netIfData.getName());
        if (resultSet.next())
        {
            if (niCached != netIfData)
            {
                resultSet.close();
                stmt.close();
                throw new ImplementationError("Two different NetInterfaceData share the same primary key", null);
            }
        }
        else
        {
            if (niCached != null)
            {
                // XXX: user deleted db entry during runtime - throw exception?
                // or just remove the item from the cache + node.removeRes(cachedRes) + warn the user?
            }
            create(con, netIfData);
        }
        resultSet.close();
        stmt.close();
    }

    @Override
    public void delete(Connection con, NetInterfaceData niData) throws SQLException
    {
        PreparedStatement stmt = con.prepareStatement(NNI_DELETE);

        stmt.setString(1, node.getName().value);
        stmt.setString(2, niData.getName().value);

        stmt.executeUpdate();
        stmt.close();

        cacheRemove(niData);
    }

    @Override
    public ObjectDatabaseDriver<InetAddress> getNetInterfaceAddressDriver()
    {
        return netIfAddressDriver;
    }

    @Override
    public ObjectDatabaseDriver<NetInterfaceType> getNetInterfaceTypeDriver()
    {
        return netIfTypeDriver;
    }


    public static List<NetInterfaceData> loadNetInterfaceData(Connection con, Node node)
        throws SQLException
    {
        PreparedStatement stmt = con.prepareStatement(NNI_SELECT_BY_NODE);
        stmt.setString(1, node.getName().value);
        ResultSet resultSet = stmt.executeQuery();

        List<NetInterfaceData> netIfDataList = new ArrayList<>();
        while (resultSet.next())
        {
            NetInterfaceName netName;
            try
            {
                netName = new NetInterfaceName(resultSet.getString(NET_DSP_NAME));
            }
            catch (InvalidNameException e)
            {
                resultSet.close();
                stmt.close();
                throw new DrbdSqlRuntimeException("NetInterface contains illegal displayName");
            }
            netIfDataList.add(restoreInstance(con, node, netName, resultSet));
        }

        resultSet.close();
        stmt.close();

        return netIfDataList;
    }

    private InetAddress getAddress(NetInterface value)
    {
        try
        {
            return value.getAddress(dbCtx);
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            throw new ImplementationError("NodeDerbyDriver should have a clone of sysCtx", accessDeniedExc);
        }
    }

    private NetInterfaceType getNetInterfaceType(NetInterface value)
    {
        try
        {
            return value.getNetInterfaceType(dbCtx);
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            throw new ImplementationError("NodeDerbyDriver should have a clone of sysCtx", accessDeniedExc);
        }
    }

    private static NetInterfaceData restoreInstance(
        Connection con,
        Node node,
        NetInterfaceName netName,
        ResultSet resultSet
    )
        throws SQLException
    {
        NetInterfaceData ret = cacheGet(resultSet);
        if (ret == null)
        {
            UUID uuid = UuidUtils.asUUID(resultSet.getBytes(NET_UUID));
            InetAddress addr;
            String type = resultSet.getString(INET_TYPE);
            try
            {
                addr = InetAddress.getByName(resultSet.getString(INET_ADDRESS));
            }
            catch (UnknownHostException unknownHostExc)
            {
                throw new DrbdSqlRuntimeException(
                    "SQL-based change to NetInterface's host caused an UnknownHostException",
                    unknownHostExc
                );
            }

            ObjectProtection objProt;
            {
                ObjectProtectionDatabaseDriver opDriver = DrbdManage.getObjectProtectionDatabaseDriver(
                    ObjectProtection.buildPath(node.getName(), netName)
                );
                objProt = opDriver.loadObjectProtection(con);
            }
            ret = new NetInterfaceData(
                uuid,
                objProt,
                netName,
                node,
                addr,
                NetInterfaceType.byValue(type)
            );
            if (cache(ret))
            {
                // restore references
            }
            else {
                ret = cacheGet(resultSet);
            }
        }

        return ret;
    }

    private synchronized static boolean cache(NetInterfaceData netInterfaceData)
    {
        PrimaryKey pk = getPk(netInterfaceData);
        boolean contains = niCache.containsKey(pk);
        if (!contains)
        {
            niCache.put(pk, netInterfaceData);
        }
        return !contains;
    }

    private static NetInterfaceData cacheGet(Node node, NetInterfaceName netName)
    {
        return niCache.get(new PrimaryKey(node.getName().value, netName.value));
    }

    private static NetInterfaceData cacheGet(ResultSet resultSet) throws SQLException
    {
        return niCache.get(new PrimaryKey(resultSet.getString(NODE_NAME), resultSet.getString(NET_NAME)));
    }

    private synchronized static void cacheRemove(NetInterfaceData niData)
    {
        niCache.remove(getPk(niData));
    }

    private static PrimaryKey getPk(NetInterfaceData niData)
    {
        return new PrimaryKey(niData.getNode().getName().value, niData.getName().value);
    }

    /**
     * this method should only be called by tests or if you want a full-reload from the database
     */
    static synchronized void clearCache()
    {
        niCache.clear();
    }

    private class NodeNetInterfaceAddressDriver extends UpdateOnlyDatabaseDriver<InetAddress>
    {
        public NodeNetInterfaceAddressDriver()
        {
            super(TBL_NODE_NET + "." + INET_ADDRESS);
        }

        @Override
        public void update(Connection con, InetAddress inetAddress) throws SQLException
        {
            PreparedStatement stmt = con.prepareStatement(NNI_UPDATE_ADR);

            stmt.setString(1, inetAddress.getHostName());
            stmt.setString(2, node.getName().value);
            stmt.setString(3, netName.value);

            stmt.executeUpdate();
            stmt.close();
        }
    }

    private class NodeNetInterfaceTypeDriver extends UpdateOnlyDatabaseDriver<NetInterfaceType>
    {
        public NodeNetInterfaceTypeDriver()
        {
            super(TBL_NODE_NET + "." + INET_TYPE);
        }

        @Override
        public void update(Connection con, NetInterfaceType type) throws SQLException
        {
            PreparedStatement stmt = con.prepareStatement(NNI_UPDATE_TYPE);

            stmt.setString(1, type.name());
            stmt.setString(2, node.getName().value);
            stmt.setString(3, netName.value);

            stmt.executeUpdate();
            stmt.close();
        }
    }
}
