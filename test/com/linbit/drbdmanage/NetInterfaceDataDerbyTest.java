package com.linbit.drbdmanage;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.linbit.ImplementationError;
import com.linbit.ObjectDatabaseDriver;
import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.NetInterface.NetInterfaceType;
import com.linbit.drbdmanage.security.DerbyBase;
import com.linbit.drbdmanage.security.ObjectProtection;
import com.linbit.utils.UuidUtils;

public class NetInterfaceDataDerbyTest extends DerbyBase
{
    private static final String SELECT_ALL_NODE_NET_INTERFACES =
        " SELECT " + UUID + ", " + NODE_NAME + ", " + NODE_NET_NAME + ", " + NODE_NET_DSP_NAME + ", " +
                     INET_ADDRESS + ", " + INET_TRANSPORT_TYPE +
        " FROM " + TBL_NODE_NET_INTERFACES;

    private final NetInterfaceName niName;
    private final NodeName nodeName;

    private final String niAddrStr = "127.0.0.1";
    private final DmIpAddress niAddr;
    private final NetInterfaceType niInterfaceType = NetInterfaceType.IP;

    private TransactionMgr transMgr;
    private Connection con;

    private Node node;

    private NetInterfaceDataDerbyDriver dbDriver;

    private java.util.UUID niUuid;
    private ObjectProtection niObjProt;
    private NetInterfaceData niData;
    private ObjectDatabaseDriver<DmIpAddress> niAddrDriver;
    private ObjectDatabaseDriver<NetInterfaceType> niTypeDriver;

    public NetInterfaceDataDerbyTest() throws Exception
    {
        nodeName = new NodeName("TestNodeName");
        niName = new NetInterfaceName("TestNetInterfaceName");
        niAddr = new DmIpAddress(niAddrStr);
    }

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
        niData = new NetInterfaceData(niUuid, niObjProt, niName, node, niAddr, niInterfaceType); // does not persist

        dbDriver = new NetInterfaceDataDerbyDriver(sysCtx, node, niName);
        niAddrDriver = dbDriver.getNetInterfaceAddressDriver();
        niTypeDriver = dbDriver.getNetInterfaceTypeDriver();
    }

    @Test
    public void testPersistSimple() throws Exception
    {
        niData.initialized();
        dbDriver.create(con, niData);
        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_NODE_NET_INTERFACES);

        ResultSet resultSet = stmt.executeQuery();
        assertTrue(resultSet.next());
        assertEquals(niUuid, UuidUtils.asUUID(resultSet.getBytes(UUID)));
        assertEquals(nodeName.value, resultSet.getString(NODE_NAME));
        assertEquals(niName.value, resultSet.getString(NODE_NET_NAME));
        assertEquals(niName.displayValue, resultSet.getString(NODE_NET_DSP_NAME));
        assertEquals(niAddrStr, resultSet.getString(INET_ADDRESS)); // TODO: gh - inetAddress does NOT contain port - implement and test
        assertEquals(niInterfaceType.name(), resultSet.getString(INET_TRANSPORT_TYPE));
        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testPersistDuplicate() throws Exception
    {
        niData.initialized();
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
    public void testPersistGetInstance() throws Exception
    {
        NetInterfaceName netInterfaceName = new NetInterfaceName("TestNetIface");
        NetInterfaceData.getInstance(
            sysCtx,
            node,
            netInterfaceName,
            niAddr,
            transMgr,
            NetInterfaceType.IP,
            true
        );
        con.commit();
        transMgr.commit();

        // we do not check if node gets created, as testPersistSimple() does that already
        // thus, we only check if the net interface got persisted
        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_NODE_NET_INTERFACES);
        ResultSet resultSet = stmt.executeQuery();
        assertTrue("Database did not persist netInterface", resultSet.next());
        assertEquals(netInterfaceName.value, resultSet.getString(NODE_NET_NAME));
        assertEquals(netInterfaceName.displayValue, resultSet.getString(NODE_NET_DSP_NAME));
        assertEquals(niAddr.getAddress(), resultSet.getString(INET_ADDRESS));
        assertEquals("IP", resultSet.getString(INET_TRANSPORT_TYPE));
        // transport: IP, RDMA, RoCE
        assertFalse("Database persisted too many netInterfaces", resultSet.next());
        resultSet.close();
        stmt.close();
    }

    @Test
    public void testLoadSimple() throws Exception
    {
        niData.initialized();
        dbDriver.create(con, niData);
        DriverUtils.clearCaches();

        NetInterfaceData netData = dbDriver.load(con);

        assertNotNull(netData);
        assertEquals(niUuid, netData.getUuid());
        assertEquals(nodeName.value, netData.getNode().getName().value);
        assertEquals(niName.value, netData.getName().value);
        assertEquals(niName.displayValue, netData.getName().displayValue);
        assertEquals(niAddrStr, netData.getAddress(sysCtx).getAddress()); // TODO: gh - inetAddress does NOT contain port - implement and test
        assertEquals(niInterfaceType, netData.getNetInterfaceType(sysCtx));
    }

    @Test
    public void testLoadRestore() throws Exception
    {
        niData.initialized();
        dbDriver.create(con, niData);
        DriverUtils.clearCaches();

        NetInterfaceData netData = dbDriver.load(con);

        assertNotNull(netData);
        assertEquals(niUuid, netData.getUuid());
        assertEquals(nodeName.value, netData.getNode().getName().value);
        assertEquals(niName.value, netData.getName().value);
        assertEquals(niName.displayValue, netData.getName().displayValue);
        assertEquals(niAddrStr, netData.getAddress(sysCtx).getAddress()); // TODO: gh - inetAddress does NOT contain port - implement and test
        assertEquals(niInterfaceType, netData.getNetInterfaceType(sysCtx));
    }

    @Test
    public void testLoadGetInstanceTwice() throws Exception
    {
        niData.initialized();
        dbDriver.create(con, niData);
        DriverUtils.clearCaches();

        NetInterfaceData netData1 = NetInterfaceData.getInstance(sysCtx, node, niName, niAddr, transMgr, niInterfaceType, false);

        assertNotNull(netData1);
        assertEquals(niUuid, netData1.getUuid());
        assertEquals(nodeName.value, netData1.getNode().getName().value);
        assertEquals(niName.value, netData1.getName().value);
        assertEquals(niName.displayValue, netData1.getName().displayValue);
        assertEquals(niAddrStr, netData1.getAddress(sysCtx).getAddress()); // TODO: gh - inetAddress does NOT contain port - implement and test
        assertEquals(niInterfaceType, netData1.getNetInterfaceType(sysCtx));

        NetInterfaceData netData2 = NetInterfaceData.getInstance(sysCtx, node, niName, niAddr, transMgr, niInterfaceType, false);
        assertTrue(netData1 == netData2);
    }

    @Test
    public void testLoadStatic() throws Exception
    {
        niData.initialized();
        dbDriver.create(con, niData);

        NetInterfaceDataDerbyDriver.clearCache();

        List<NetInterfaceData> niList = NetInterfaceDataDerbyDriver.loadNetInterfaceData(con, node);
        assertEquals(1, niList.size());
        NetInterfaceData netData = niList.get(0);

        assertNotNull(netData);
        assertEquals(niUuid, netData.getUuid());
        assertEquals(nodeName.value, netData.getNode().getName().value);
        assertEquals(niName.value, netData.getName().value);
        assertEquals(niName.displayValue, netData.getName().displayValue);
        assertEquals(niAddrStr, netData.getAddress(sysCtx).getAddress()); // TODO: gh - inetAddress does NOT contain port - implement and test
        assertEquals(niInterfaceType, netData.getNetInterfaceType(sysCtx));
        assertNotNull(netData.getObjProt());
    }

    @Test
    public void testCache() throws Exception
    {
        dbDriver.create(con, niData);

        // no clearCaches
        assertEquals(niData, dbDriver.load(con));
    }

    @Test
    public void testDeleteSimple() throws Exception
    {
        niData.initialized();
        dbDriver.create(con, niData);

        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_NODE_NET_INTERFACES);
        ResultSet resultSet = stmt.executeQuery();
        assertTrue(resultSet.next());
        resultSet.close();

        dbDriver.delete(con);

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

        niData.initialized();
        dbDriver.ensureEntryExists(con, niData);

        assertTrue(stmt.executeQuery().next());

        dbDriver.ensureEntryExists(con, niData);

        assertTrue(stmt.executeQuery().next());
        stmt.close();
    }

    @Test (expected = ImplementationError.class)
    public void testAddrDelete() throws Exception
    {
        niData.initialized();
        dbDriver.create(con, niData);
        niAddrDriver.delete(con, niAddr);
    }

    @Test (expected = ImplementationError.class)
    public void testAddrInsert() throws Exception
    {
        niData.initialized();
        dbDriver.create(con, niData);
        niAddrDriver.insert(con, niAddr);
    }

    @Test
    public void testAddrUpdate() throws Exception
    {
        niData.initialized();
        dbDriver.create(con, niData);

        String addrStr = "::1";
        DmIpAddress addr = new DmIpAddress(addrStr);

        niData.setConnection(transMgr);
        niData.setAddress(sysCtx, addr);
        transMgr.commit();

        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_NODE_NET_INTERFACES);
        ResultSet resultSet = stmt.executeQuery();
        assertTrue(resultSet.next());
        assertEquals(addrStr, resultSet.getString(INET_ADDRESS)); // TODO: gh - inetAddress does NOT contain port - implement and test
        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }


    @Test
    public void testAddrUpdateDriver() throws Exception
    {
        niData.initialized();
        dbDriver.create(con, niData);
        String addrStr = "::1";
        DmIpAddress addr = new DmIpAddress(addrStr);
        niAddrDriver.update(con, addr);

        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_NODE_NET_INTERFACES);
        ResultSet resultSet = stmt.executeQuery();
        assertTrue(resultSet.next());
        assertEquals(addrStr, resultSet.getString(INET_ADDRESS)); // TODO: gh - inetAddress does NOT contain port - implement and test
        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test (expected = ImplementationError.class)
    public void testTypeDelete() throws Exception
    {
        niData.initialized();
        dbDriver.create(con, niData);
        niTypeDriver.delete(con, niInterfaceType);
    }

    @Test (expected = ImplementationError.class)
    public void testTypeInsert() throws Exception
    {
        niData.initialized();
        dbDriver.create(con, niData);
        niTypeDriver.insert(con, niInterfaceType);
    }

    @Test
    public void testTypeUpdate() throws Exception
    {
        niData.initialized();
        dbDriver.create(con, niData);

        niData.setConnection(transMgr);
        niData.setNetInterfaceType(sysCtx, NetInterfaceType.RDMA);
        transMgr.commit();

        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_NODE_NET_INTERFACES);
        ResultSet resultSet = stmt.executeQuery();
        assertTrue(resultSet.next());
        assertEquals(NetInterfaceType.RDMA.name(), resultSet.getString(INET_TRANSPORT_TYPE));
        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testTypeUpdateDriver() throws Exception
    {
        niData.initialized();
        dbDriver.create(con, niData);
        niTypeDriver.update(con, NetInterfaceType.RDMA);

        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_NODE_NET_INTERFACES);
        ResultSet resultSet = stmt.executeQuery();
        assertTrue(resultSet.next());
        assertEquals(NetInterfaceType.RDMA.name(), resultSet.getString(INET_TRANSPORT_TYPE));
        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testGetInstanceSatelliteCreate() throws Exception
    {
        DriverUtils.satelliteMode();

        NetInterfaceData netData = NetInterfaceData.getInstance(sysCtx, node, niName, niAddr, null, niInterfaceType, true);

        assertNotNull(netData);
        assertEquals(niAddr, netData.getAddress(sysCtx));
        assertEquals(niName, netData.getName());
        assertEquals(niInterfaceType, netData.getNetInterfaceType(sysCtx));
        assertEquals(node, netData.getNode());
        assertNotNull(netData.getObjProt());
        assertNotNull(netData.getUuid());

        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_NODE_NET_INTERFACES);
        ResultSet resultSet = stmt.executeQuery();
        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testGetInstanceSatelliteNoCreate() throws Exception
    {
        DriverUtils.satelliteMode();

        NetInterfaceData netData = NetInterfaceData.getInstance(sysCtx, node, niName, niAddr, null, niInterfaceType, false);

        assertNull(netData);

        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_NODE_NET_INTERFACES);
        ResultSet resultSet = stmt.executeQuery();
        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testEnsureExistsDoublePrimaryKey() throws Exception
    {
        niData.initialized();
        dbDriver.ensureEntryExists(con, niData);

        NetInterfaceData netData = new NetInterfaceData(
            randomUUID(),
            null,
            niName,
            node,
            niAddr,
            niInterfaceType
        );
        try
        {
            dbDriver.ensureEntryExists(con, netData);
            fail("dbDriver should have thrown ImplementationError");
        }
        catch (ImplementationError implErr)
        {
            // expected
        }
    }

    @Test
    public void testHalfValidName() throws Exception
    {
        dbDriver.create(con, niData);
        DriverUtils.clearCaches();

        NetInterfaceName halfValidNiName = new NetInterfaceName(niData.getName().value);
        NetInterfaceDataDerbyDriver driver = new NetInterfaceDataDerbyDriver(sysCtx, node, halfValidNiName);

        NetInterfaceData loadedNi = driver.load(con);

        assertNotNull(loadedNi);
        assertEquals(niData.getName(), loadedNi.getName());
        assertEquals(niData.getUuid(), loadedNi.getUuid());
    }
}
