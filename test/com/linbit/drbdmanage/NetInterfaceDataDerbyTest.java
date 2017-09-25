package com.linbit.drbdmanage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.junit.Test;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.NetInterface.NetInterfaceType;
import com.linbit.drbdmanage.core.CoreUtils;
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

    private Node node;

    private NetInterfaceDataDerbyDriver dbDriver;

    private java.util.UUID niUuid;
    private ObjectProtection niObjProt;
    private NetInterfaceData niData;
    private SingleColumnDatabaseDriver<NetInterfaceData, DmIpAddress> niAddrDriver;
    private SingleColumnDatabaseDriver<NetInterfaceData, NetInterfaceType> niTypeDriver;

    public NetInterfaceDataDerbyTest() throws Exception
    {
        nodeName = new NodeName("TestNodeName");
        niName = new NetInterfaceName("TestNetInterfaceName");
        niAddr = new DmIpAddress(niAddrStr);
    }

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        assertEquals("NODE_NET_INTERFACES table's column count has changed. Update tests accordingly!", 6, TBL_COL_COUNT_NODE_NET_INTERFACES);

        transMgr = new TransactionMgr(getConnection());

        node = NodeData.getInstance(
            sysCtx,
            nodeName,
            null, // types
            null, // flags
            transMgr,
            true
        );

        niUuid = java.util.UUID.randomUUID();
        niObjProt = ObjectProtection.getInstance(
            sysCtx,
            ObjectProtection.buildPath(nodeName, niName),
            true,
            transMgr
        );
        niData = new NetInterfaceData(niUuid, niObjProt, niName, node, niAddr, niInterfaceType); // does not persist

        dbDriver = new NetInterfaceDataDerbyDriver(sysCtx, errorReporter);
        niAddrDriver = dbDriver.getNetInterfaceAddressDriver();
        niTypeDriver = dbDriver.getNetInterfaceTypeDriver();
    }

    @Test
    public void testPersistSimple() throws Exception
    {
        niData.initialized();
        dbDriver.create(niData, transMgr);
        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_NODE_NET_INTERFACES);

        ResultSet resultSet = stmt.executeQuery();
        assertTrue(resultSet.next());
        assertEquals(niUuid, UuidUtils.asUuid(resultSet.getBytes(UUID)));
        assertEquals(nodeName.value, resultSet.getString(NODE_NAME));
        assertEquals(niName.value, resultSet.getString(NODE_NET_NAME));
        assertEquals(niName.displayValue, resultSet.getString(NODE_NET_DSP_NAME));
        assertEquals(niAddrStr, resultSet.getString(INET_ADDRESS));
        assertEquals(niInterfaceType.name(), resultSet.getString(INET_TRANSPORT_TYPE));
        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testPersistDuplicate() throws Exception
    {
        niData.initialized();
        dbDriver.create(niData, transMgr);
        try
        {
            dbDriver.create(niData, transMgr);
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
        transMgr.commit();

        // we do not check if node gets created, as testPersistSimple() does that already
        // thus, we only check if the net interface got persisted
        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_NODE_NET_INTERFACES);
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
        dbDriver.create(niData, transMgr);

        NetInterfaceData netData = dbDriver.load(node, niName, transMgr);

        assertNotNull(netData);
        assertEquals(niUuid, netData.getUuid());
        assertEquals(nodeName.value, netData.getNode().getName().value);
        assertEquals(niName.value, netData.getName().value);
        assertEquals(niName.displayValue, netData.getName().displayValue);
        assertEquals(niAddrStr, netData.getAddress(sysCtx).getAddress());
        assertEquals(niInterfaceType, netData.getNetInterfaceType(sysCtx));
    }

    @Test
    public void testLoadRestore() throws Exception
    {
        niData.initialized();
        dbDriver.create(niData, transMgr);

        NetInterfaceData netData = dbDriver.load(node, niName, transMgr);

        assertNotNull(netData);
        assertEquals(niUuid, netData.getUuid());
        assertEquals(nodeName.value, netData.getNode().getName().value);
        assertEquals(niName.value, netData.getName().value);
        assertEquals(niName.displayValue, netData.getName().displayValue);
        assertEquals(niAddrStr, netData.getAddress(sysCtx).getAddress());
        assertEquals(niInterfaceType, netData.getNetInterfaceType(sysCtx));
    }

    @Test
    public void testLoadGetInstanceTwice() throws Exception
    {
        niData.initialized();
        dbDriver.create(niData, transMgr);

        NetInterfaceData netData1 = NetInterfaceData.getInstance(sysCtx, node, niName, niAddr, transMgr, niInterfaceType, false);

        assertNotNull(netData1);
        assertEquals(niUuid, netData1.getUuid());
        assertEquals(nodeName.value, netData1.getNode().getName().value);
        assertEquals(niName.value, netData1.getName().value);
        assertEquals(niName.displayValue, netData1.getName().displayValue);
        assertEquals(niAddrStr, netData1.getAddress(sysCtx).getAddress());
        assertEquals(niInterfaceType, netData1.getNetInterfaceType(sysCtx));

        NetInterfaceData netData2 = NetInterfaceData.getInstance(sysCtx, node, niName, niAddr, transMgr, niInterfaceType, false);
        assertTrue(netData1 == netData2);
    }

    @Test
    public void testLoadAll() throws Exception
    {
        niData.initialized();
        dbDriver.create(niData, transMgr);

        List<NetInterfaceData> niList = dbDriver.loadNetInterfaceData(node, transMgr);
        assertEquals(1, niList.size());
        NetInterfaceData netData = niList.get(0);

        assertNotNull(netData);
        assertEquals(niUuid, netData.getUuid());
        assertEquals(nodeName.value, netData.getNode().getName().value);
        assertEquals(niName.value, netData.getName().value);
        assertEquals(niName.displayValue, netData.getName().displayValue);
        assertEquals(niAddrStr, netData.getAddress(sysCtx).getAddress());
        assertEquals(niInterfaceType, netData.getNetInterfaceType(sysCtx));
        assertNotNull(netData.getObjProt());
    }

    @Test
    public void testCache() throws Exception
    {
        NetInterfaceData storedInstance = NetInterfaceData.getInstance(
            sysCtx,
            node,
            niName,
            niAddr,
            transMgr,
            niInterfaceType,
            true
        );

        // no clearCaches
        assertEquals(storedInstance, dbDriver.load(node, niName, transMgr));
    }

    @Test
    public void testDeleteSimple() throws Exception
    {
        niData.initialized();
        dbDriver.create(niData, transMgr);

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_NODE_NET_INTERFACES);
        ResultSet resultSet = stmt.executeQuery();
        assertTrue(resultSet.next());
        resultSet.close();

        dbDriver.delete(niData, transMgr);

        resultSet = stmt.executeQuery();
        assertFalse(resultSet.next());
        resultSet.close();
        stmt.close();
    }

    @Test
    public void testEnsureExists() throws Exception
    {
        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_NODE_NET_INTERFACES);
        assertFalse(stmt.executeQuery().next());

        niData.initialized();
        dbDriver.ensureEntryExists(niData, transMgr);

        assertTrue(stmt.executeQuery().next());

        dbDriver.ensureEntryExists(niData, transMgr);

        assertTrue(stmt.executeQuery().next());
        stmt.close();
    }

    @Test
    public void testAddrUpdateInstance() throws Exception
    {
        niData.initialized();
        dbDriver.create(niData, transMgr);

        String addrStr = "::1";
        DmIpAddress addr = new DmIpAddress(addrStr);

        niData.setConnection(transMgr);
        niData.setAddress(sysCtx, addr);
        transMgr.commit();

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_NODE_NET_INTERFACES);
        ResultSet resultSet = stmt.executeQuery();
        assertTrue(resultSet.next());
        assertEquals(addrStr, resultSet.getString(INET_ADDRESS));
        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }


    @Test
    public void testAddrUpdateDriver() throws Exception
    {
        niData.initialized();
        dbDriver.create(niData, transMgr);
        String addrStr = "::1";
        DmIpAddress addr = new DmIpAddress(addrStr);
        niAddrDriver.update(niData, addr, transMgr);

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_NODE_NET_INTERFACES);
        ResultSet resultSet = stmt.executeQuery();
        assertTrue(resultSet.next());
        assertEquals(addrStr, resultSet.getString(INET_ADDRESS));
        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testTypeUpdate() throws Exception
    {
        niData.initialized();
        dbDriver.create(niData, transMgr);

        niData.setConnection(transMgr);
        niData.setNetInterfaceType(sysCtx, NetInterfaceType.RDMA);
        transMgr.commit();

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_NODE_NET_INTERFACES);
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
        dbDriver.create(niData, transMgr);
        niTypeDriver.update(niData, NetInterfaceType.RDMA, transMgr);

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_NODE_NET_INTERFACES);
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
        CoreUtils.satelliteMode();

        NetInterfaceData netData = NetInterfaceData.getInstance(sysCtx, node, niName, niAddr, null, niInterfaceType, true);

        assertNotNull(netData);
        assertEquals(niAddr, netData.getAddress(sysCtx));
        assertEquals(niName, netData.getName());
        assertEquals(niInterfaceType, netData.getNetInterfaceType(sysCtx));
        assertEquals(node, netData.getNode());
        assertNotNull(netData.getObjProt());
        assertNotNull(netData.getUuid());

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_NODE_NET_INTERFACES);
        ResultSet resultSet = stmt.executeQuery();
        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testGetInstanceSatelliteNoCreate() throws Exception
    {
        CoreUtils.satelliteMode();

        NetInterfaceData netData = NetInterfaceData.getInstance(sysCtx, node, niName, niAddr, null, niInterfaceType, false);

        assertNull(netData);

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_NODE_NET_INTERFACES);
        ResultSet resultSet = stmt.executeQuery();
        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testHalfValidName() throws Exception
    {
        dbDriver.create(niData, transMgr);

        NetInterfaceName halfValidNiName = new NetInterfaceName(niData.getName().value);

        NetInterfaceData loadedNi = dbDriver.load(node, halfValidNiName, transMgr);

        assertNotNull(loadedNi);
        assertEquals(niData.getName(), loadedNi.getName());
        assertEquals(niData.getUuid(), loadedNi.getUuid());
    }
}
