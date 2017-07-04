package com.linbit.drbdmanage;

import static org.junit.Assert.*;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ObjectDatabaseDriver;
import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.NetInterface.NetInterfaceType;
import com.linbit.drbdmanage.security.DerbyBase;
import com.linbit.drbdmanage.security.ObjectProtection;
import com.linbit.utils.UuidUtils;

public class DerbyNetInterfaceDataTest extends DerbyBase
{
    private static final NetInterfaceName niName;
    private static final NodeName nodeName;

    private static final String niAddrStr = "127.0.0.1";
    private static final InetAddress niAddr;
    private static final NetInterfaceType niTransportType = NetInterfaceType.IP;

    private static final String SELECT_ALL_NODE_NET_INTERFACES =
        " SELECT " + UUID + ", " + NODE_NAME + ", " + NODE_NET_NAME + ", " + NODE_NET_DSP_NAME + ", " +
            INET_ADDRESS + ", " + INET_TRANSPORT_TYPE +
        " FROM " + TBL_NODE_NET_INTERFACES;

    static
    {
        try
        {
            nodeName = new NodeName("TestNodeName");
            niName = new NetInterfaceName("TestNetInterfaceName");
            niAddr = InetAddress.getByName(niAddrStr);
        }
        catch (InvalidNameException | UnknownHostException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private TransactionMgr transMgr;
    private Connection con;

    private Node node;

    private NetInterfaceDataDerbyDriver dbDriver;

    private java.util.UUID niUuid;
    private ObjectProtection niObjProt;
    private NetInterfaceData niData;
    private ObjectDatabaseDriver<InetAddress> niAddrDriver;
    private ObjectDatabaseDriver<NetInterfaceType> niTypeDriver;

    @Before
    public void startUp() throws Exception
    {
        assertEquals("NODE_NET_INTERFACES table's column count has changed. Update tests accordingly!", 6, TBL_COL_COUNT_NODE_NET_INTERFACES);

        con = getConnection();
        transMgr = new TransactionMgr(con);

        node = NodeData.getInstance(
            sysCtx,
            nodeName,
            null, // types
            null, // flags
            null, // srlGen
            transMgr,
            true
        );

        niUuid = java.util.UUID.randomUUID();
        niObjProt = ObjectProtection.getInstance(
            sysCtx,
            transMgr,
            ObjectProtection.buildPath(nodeName, niName),
            true
        );
        niData = new NetInterfaceData(niUuid, niObjProt, niName, node, niAddr, niTransportType); // does not persist

        dbDriver = new NetInterfaceDataDerbyDriver(sysCtx, node, niName);
        niAddrDriver = dbDriver.getNetInterfaceAddressDriver();
        niTypeDriver = dbDriver.getNetInterfaceTypeDriver();
    }

    @Test
    public void testPersistSimple() throws Exception
    {
        dbDriver.create(con, niData);
        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_NODE_NET_INTERFACES);

        ResultSet resultSet = stmt.executeQuery();
        assertTrue(resultSet.next());
        assertEquals(niUuid, UuidUtils.asUUID(resultSet.getBytes(UUID)));
        assertEquals(nodeName.value, resultSet.getString(NODE_NAME));
        assertEquals(niName.value, resultSet.getString(NODE_NET_NAME));
        assertEquals(niName.displayValue, resultSet.getString(NODE_NET_DSP_NAME));
        assertEquals(niAddrStr, resultSet.getString(INET_ADDRESS)); // TODO: gh - inetAddress does NOT contain port - implement and test
        assertEquals(niTransportType.name(), resultSet.getString(INET_TRANSPORT_TYPE));
        assertFalse(resultSet.next());
    }

    @Test
    public void testPersistDuplicate() throws Exception
    {
        dbDriver.create(con, niData);
        try
        {
            dbDriver.create(con, niData);
            fail("driver persisted same object twice - exception expected");
        }
        catch (SQLException exc)
        {
            // expected
        }
    }

    @Test
    public void testLoadSimple() throws Exception
    {
        dbDriver.create(con, niData);

        NetInterfaceData netData = dbDriver.load(con);

        assertNotNull(netData);
        assertEquals(niUuid, netData.getUuid());
        assertEquals(nodeName.value, netData.getNode().getName().value);
        assertEquals(niName.value, netData.getName().value);
        assertEquals(niName.displayValue, netData.getName().displayValue);
        assertEquals(niAddrStr, netData.getAddress(sysCtx).getHostAddress()); // TODO: gh - inetAddress does NOT contain port - implement and test
        assertEquals(niTransportType, netData.getNetInterfaceType(sysCtx));
    }

    @Test
    public void testLoadStatic() throws Exception
    {
        dbDriver.create(con, niData);

        List<NetInterfaceData> niList = NetInterfaceDataDerbyDriver.loadNetInterfaceData(con, node);
        assertEquals(1, niList.size());
        NetInterfaceData netData = niList.get(0);

        assertNotNull(netData);
        assertEquals(niUuid, netData.getUuid());
        assertEquals(nodeName.value, netData.getNode().getName().value);
        assertEquals(niName.value, netData.getName().value);
        assertEquals(niName.displayValue, netData.getName().displayValue);
        assertEquals(niAddrStr, netData.getAddress(sysCtx).getHostAddress()); // TODO: gh - inetAddress does NOT contain port - implement and test
        assertEquals(niTransportType, netData.getNetInterfaceType(sysCtx));
    }

    @Test
    public void testDeleteSimple() throws Exception
    {
        dbDriver.create(con, niData);

        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_NODE_NET_INTERFACES);
        ResultSet resultSet = stmt.executeQuery();
        assertTrue(resultSet.next());
        resultSet.close();

        dbDriver.delete(con, niData);

        resultSet = stmt.executeQuery();
        assertFalse(resultSet.next());
        resultSet.close();
        stmt.close();
    }

    @Test
    public void testEnsureExists() throws Exception
    {
        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_NODE_NET_INTERFACES);
        assertFalse(stmt.executeQuery().next());

        dbDriver.ensureEntryExists(con, niData);

        assertTrue(stmt.executeQuery().next());

        dbDriver.ensureEntryExists(con, niData);

        assertTrue(stmt.executeQuery().next());
        stmt.close();
    }

    @Test (expected = ImplementationError.class)
    public void testAddrDelete() throws Exception
    {
        dbDriver.create(con, niData);
        niAddrDriver.delete(con, niAddr);
    }

    @Test (expected = ImplementationError.class)
    public void testAddrInsert() throws Exception
    {
        dbDriver.create(con, niData);
        niAddrDriver.insert(con, niAddr);
    }

    @Test
    public void testAddrUpdate() throws Exception
    {
        dbDriver.create(con, niData);
        String addrStr = "::1";
        InetAddress addr = InetAddress.getByName(addrStr);
        niAddrDriver.update(con, addr);

        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_NODE_NET_INTERFACES);
        ResultSet resultSet = stmt.executeQuery();
        assertTrue(resultSet.next());
        assertEquals(addrStr, resultSet.getString(INET_ADDRESS)); // TODO: gh - inetAddress does NOT contain port - implement and test
        assertFalse(resultSet.next());
    }

    @Test (expected = ImplementationError.class)
    public void testTypeDelete() throws Exception
    {
        dbDriver.create(con, niData);
        niTypeDriver.delete(con, niTransportType);
    }

    @Test (expected = ImplementationError.class)
    public void testTypeInsert() throws Exception
    {
        dbDriver.create(con, niData);
        niTypeDriver.insert(con, niTransportType);
    }

    @Test
    public void testTypeUpdate() throws Exception
    {
        dbDriver.create(con, niData);
        niTypeDriver.update(con, NetInterfaceType.RDMA);

        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_NODE_NET_INTERFACES);
        ResultSet resultSet = stmt.executeQuery();
        assertTrue(resultSet.next());
        assertEquals(NetInterfaceType.RDMA.name(), resultSet.getString(INET_TRANSPORT_TYPE));
        assertFalse(resultSet.next());
    }

}
