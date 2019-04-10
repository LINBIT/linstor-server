package com.linbit.linstor;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.NetInterface.EncryptionType;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.security.GenericDbBase;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class NetInterfaceDataGenericDbDriverTest extends GenericDbBase
{
    private static final String SELECT_ALL_NODE_NET_INTERFACES =
        " SELECT " + UUID + ", " + NODE_NAME + ", " + NODE_NET_NAME + ", " + NODE_NET_DSP_NAME + ", " +
                     INET_ADDRESS +
        " FROM " + TBL_NODE_NET_INTERFACES;

    private final NetInterfaceName niName;
    private final NodeName nodeName;

    private final String niAddrStr = "127.0.0.1";
    private final LsIpAddress niAddr;

    private final TcpPortNumber niStltConnPort;
    private final EncryptionType niStltConnEncrType;

    private Node node;

    private NetInterfaceDataGenericDbDriver dbDriver;

    private java.util.UUID niUuid;
    private NetInterfaceData niData;
    private SingleColumnDatabaseDriver<NetInterfaceData, LsIpAddress> niAddrDriver;

    public NetInterfaceDataGenericDbDriverTest() throws Exception
    {
        nodeName = new NodeName("TestNodeName");
        niName = new NetInterfaceName("TestNetInterfaceName");
        niAddr = new LsIpAddress(niAddrStr);
        niStltConnPort = new TcpPortNumber(ApiConsts.DFLT_STLT_PORT_PLAIN);
        niStltConnEncrType = EncryptionType.PLAIN;
    }

    @SuppressWarnings("checkstyle:magicnumber")
    @Before
    public void setUp() throws Exception
    {
        super.setUpAndEnterScope();
        assertEquals(
            TBL_NODE_NET_INTERFACES + " table's column count has changed. Update tests accordingly!",
            5,
            TBL_COL_COUNT_NODE_NET_INTERFACES
        );

        node = nodeDataFactory.create(
            SYS_CTX,
            nodeName,
            null, // types
            null // flags
        );

        dbDriver = new NetInterfaceDataGenericDbDriver(SYS_CTX, errorReporter, transObjFactory, transMgrProvider);
        niAddrDriver = dbDriver.getNetInterfaceAddressDriver();

        niUuid = java.util.UUID.randomUUID();

        // not persisted
        niData = new NetInterfaceData(
            niUuid,
            niName,
            node,
            niAddr,
            niStltConnPort,
            niStltConnEncrType,
            dbDriver,
            transObjFactory,
            transMgrProvider
        );
    }

    @Test
    public void testPersistSimple() throws Exception
    {
        dbDriver.create(niData);
        commit();

        PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_NODE_NET_INTERFACES);

        ResultSet resultSet = stmt.executeQuery();
        assertTrue(resultSet.next());
        assertEquals(niUuid, java.util.UUID.fromString(resultSet.getString(UUID)));
        assertEquals(nodeName.value, resultSet.getString(NODE_NAME));
        assertEquals(niName.value, resultSet.getString(NODE_NET_NAME));
        assertEquals(niName.displayValue, resultSet.getString(NODE_NET_DSP_NAME));
        assertEquals(niAddrStr, resultSet.getString(INET_ADDRESS));
        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testPersistDuplicate() throws Exception
    {
        dbDriver.create(niData);
        try
        {
            dbDriver.create(niData);
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
        netInterfaceDataFactory.create(
            SYS_CTX,
            node,
            netInterfaceName,
            niAddr,
            niStltConnPort,
            niStltConnEncrType
        );
        commit();

        // we do not check if node gets created, as testPersistSimple() does that already
        // thus, we only check if the net interface got persisted
        PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_NODE_NET_INTERFACES);
        ResultSet resultSet = stmt.executeQuery();
        assertTrue("Database did not persist netInterface", resultSet.next());
        assertEquals(netInterfaceName.value, resultSet.getString(NODE_NET_NAME));
        assertEquals(netInterfaceName.displayValue, resultSet.getString(NODE_NET_DSP_NAME));
        assertEquals(niAddr.getAddress(), resultSet.getString(INET_ADDRESS));
        assertFalse("Database persisted too many netInterfaces", resultSet.next());
        resultSet.close();
        stmt.close();
    }


    @Test
    public void testLoadRestore() throws Exception
    {
        dbDriver.create(niData);

        Map<NodeName, Node> tmpNodesMap = new HashMap<>();
        tmpNodesMap.put(nodeName, node);
        List<NetInterfaceData> niList = dbDriver.loadAll(tmpNodesMap);
        assertNotNull(niList);
        assertEquals(1, niList.size());

        NetInterfaceData netData = niList.get(0);

        assertNotNull(netData);
        assertEquals(niUuid, netData.getUuid());
        assertEquals(nodeName.value, netData.getNode().getName().value);
        assertEquals(niName.value, netData.getName().value);
        assertEquals(niName.displayValue, netData.getName().displayValue);
        assertEquals(niAddrStr, netData.getAddress(SYS_CTX).getAddress());
    }

    @Test
    public void testLoadGetInstanceTwice() throws Exception
    {
        dbDriver.create(niData);
        ((NodeData) node).addNetInterface(SYS_CTX, niData);

        NetInterfaceData netData1 = (NetInterfaceData) node.getNetInterface(SYS_CTX, niName);

        assertNotNull(netData1);
        assertEquals(niUuid, netData1.getUuid());
        assertEquals(nodeName.value, netData1.getNode().getName().value);
        assertEquals(niName.value, netData1.getName().value);
        assertEquals(niName.displayValue, netData1.getName().displayValue);
        assertEquals(niAddrStr, netData1.getAddress(SYS_CTX).getAddress());

        NetInterfaceData netData2 = (NetInterfaceData) node.getNetInterface(SYS_CTX, niName);
        assertTrue(netData1 == netData2);
    }

    @Test
    public void testLoadAll() throws Exception
    {
        dbDriver.create(niData);
        nodesMap.put(nodeName, node);

        List<NetInterfaceData> niList = dbDriver.loadAll(nodesMap);
        assertEquals(1, niList.size());
        NetInterfaceData netData = niList.get(0);

        assertNotNull(netData);
        assertEquals(niUuid, netData.getUuid());
        assertEquals(nodeName.value, netData.getNode().getName().value);
        assertEquals(niName.value, netData.getName().value);
        assertEquals(niName.displayValue, netData.getName().displayValue);
        assertEquals(niAddrStr, netData.getAddress(SYS_CTX).getAddress());
    }

    @Test
    public void testCache() throws Exception
    {
        NetInterfaceData storedInstance = netInterfaceDataFactory.create(
            SYS_CTX,
            node,
            niName,
            niAddr,
            niStltConnPort,
            niStltConnEncrType
        );

        // no clearCaches
        assertEquals(storedInstance, node.getNetInterface(SYS_CTX, niName));
    }

    @Test
    public void testDeleteSimple() throws Exception
    {
        dbDriver.create(niData);
        commit();

        PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_NODE_NET_INTERFACES);
        ResultSet resultSet = stmt.executeQuery();
        assertTrue(resultSet.next());
        resultSet.close();

        dbDriver.delete(niData);

        resultSet = stmt.executeQuery();
        assertFalse(resultSet.next());
        resultSet.close();
        stmt.close();
    }

    @Test
    public void testEnsureExists() throws Exception
    {
        PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_NODE_NET_INTERFACES);
        assertFalse(stmt.executeQuery().next());

        dbDriver.ensureEntryExists(niData);
        commit();

        assertTrue(stmt.executeQuery().next());

        dbDriver.ensureEntryExists(niData);
        commit();

        assertTrue(stmt.executeQuery().next());
        stmt.close();
    }

    @Test
    public void testAddrUpdateInstance() throws Exception
    {
        dbDriver.create(niData);

        String addrStr = "::1";
        LsIpAddress addr = new LsIpAddress(addrStr);

        niData.setAddress(SYS_CTX, addr);
        commit();

        PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_NODE_NET_INTERFACES);
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
        dbDriver.create(niData);
        commit();

        String addrStr = "::1";
        LsIpAddress addr = new LsIpAddress(addrStr);
        niAddrDriver.update(niData, addr);
        commit();

        PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_NODE_NET_INTERFACES);
        ResultSet resultSet = stmt.executeQuery();
        assertTrue(resultSet.next());
        assertEquals(addrStr, resultSet.getString(INET_ADDRESS));
        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test (expected = LinStorDataAlreadyExistsException.class)
    public void testAlreadyExists() throws Exception
    {
        dbDriver.create(niData);
        ((NodeData) node).addNetInterface(SYS_CTX, niData);

        netInterfaceDataFactory.create(
            SYS_CTX,
            node,
            niName,
            niAddr,
            niStltConnPort,
            niStltConnEncrType
        );
    }

    // TODO: add tests for StltConnPort and StltConnEncrType
}
