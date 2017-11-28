package com.linbit.linstor;

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
import com.linbit.linstor.LsIpAddress;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.NetInterfaceData;
import com.linbit.linstor.NetInterfaceDataDerbyDriver;
import com.linbit.linstor.NetInterfaceName;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.NetInterface.NetInterfaceType;
import com.linbit.linstor.security.DerbyBase;
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
    private final LsIpAddress niAddr;
    private final int port = 9001;
    private final NetInterfaceType niInterfaceType = NetInterfaceType.IP;

    private TransactionMgr transMgr;

    private Node node;

    private NetInterfaceDataDerbyDriver dbDriver;

    private java.util.UUID niUuid;
    private NetInterfaceData niData;
    private SingleColumnDatabaseDriver<NetInterfaceData, LsIpAddress> niAddrDriver;
    private SingleColumnDatabaseDriver<NetInterfaceData, NetInterfaceType> niTypeDriver;

    public NetInterfaceDataDerbyTest() throws Exception
    {
        nodeName = new NodeName("TestNodeName");
        niName = new NetInterfaceName("TestNetInterfaceName");
        niAddr = new LsIpAddress(niAddrStr);
    }

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        assertEquals("NODE_NET_INTERFACES table's column count has changed. Update tests accordingly!", 7, TBL_COL_COUNT_NODE_NET_INTERFACES);

        transMgr = new TransactionMgr(getConnection());

        node = NodeData.getInstance(
            sysCtx,
            nodeName,
            null, // types
            null, // flags
            transMgr,
            true,
            false
        );

        niUuid = java.util.UUID.randomUUID();
        niData = new NetInterfaceData(niUuid, sysCtx, niName, node, niAddr, port, niInterfaceType); // does not persist

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
            port,
            NetInterfaceType.IP,
            transMgr,
            true,
            false
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

        NetInterfaceData netData = dbDriver.load(node, niName, true, transMgr);

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

        NetInterfaceData netData = dbDriver.load(node, niName, true, transMgr);

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

        NetInterfaceData netData1 = NetInterfaceData.getInstance(
            sysCtx,
            node,
            niName,
            niAddr,
            port,
            niInterfaceType,
            transMgr,
            false,
            false
        );

        assertNotNull(netData1);
        assertEquals(niUuid, netData1.getUuid());
        assertEquals(nodeName.value, netData1.getNode().getName().value);
        assertEquals(niName.value, netData1.getName().value);
        assertEquals(niName.displayValue, netData1.getName().displayValue);
        assertEquals(niAddrStr, netData1.getAddress(sysCtx).getAddress());
        assertEquals(niInterfaceType, netData1.getNetInterfaceType(sysCtx));

        NetInterfaceData netData2 = NetInterfaceData.getInstance(
            sysCtx,
            node,
            niName,
            niAddr,
            port,
            niInterfaceType,
            transMgr,
            false,
            false
        );
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
    }

    @Test
    public void testCache() throws Exception
    {
        NetInterfaceData storedInstance = NetInterfaceData.getInstance(
            sysCtx,
            node,
            niName,
            niAddr,
            port,
            niInterfaceType,
            transMgr,
            true,
            false
        );

        // no clearCaches
        assertEquals(storedInstance, dbDriver.load(node, niName, true, transMgr));
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
        LsIpAddress addr = new LsIpAddress(addrStr);

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
        LsIpAddress addr = new LsIpAddress(addrStr);
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
        satelliteMode();

        NetInterfaceData netData = NetInterfaceData.getInstance(
            sysCtx,
            node,
            niName,
            niAddr,
            port,
            niInterfaceType,
            null,
            true,
            false
        );

        assertNotNull(netData);
        assertEquals(niAddr, netData.getAddress(sysCtx));
        assertEquals(niName, netData.getName());
        assertEquals(niInterfaceType, netData.getNetInterfaceType(sysCtx));
        assertEquals(node, netData.getNode());
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
        satelliteMode();

        NodeName nodeName2 = new NodeName("OtherNodeName");
        NodeData node2 = NodeData.getInstance(
            sysCtx,
            nodeName2,
            null, // types
            null, // flags
            transMgr,
            true,
            false
        );

        NetInterfaceData netData = NetInterfaceData.getInstance(
            sysCtx,
            node2,
            niName,
            niAddr,
            port,
            niInterfaceType,
            null,
            false,
            false
        );

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

        NetInterfaceData loadedNi = dbDriver.load(node, halfValidNiName, true, transMgr);

        assertNotNull(loadedNi);
        assertEquals(niData.getName(), loadedNi.getName());
        assertEquals(niData.getUuid(), loadedNi.getUuid());
    }

    @Test (expected = LinStorDataAlreadyExistsException.class)
    public void testAlreadyExists() throws Exception
    {
        dbDriver.create(niData, transMgr);

        NetInterfaceData.getInstance(
            sysCtx,
            node,
            niName,
            niAddr,
            port,
            niInterfaceType,
            transMgr,
            false,
            true
        );
    }
}
