package com.linbit.linstor;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.security.DerbyBase;
import com.linbit.utils.UuidUtils;
import org.junit.Before;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class NetInterfaceDataDerbyTest extends DerbyBase
{
    private static final String SELECT_ALL_NODE_NET_INTERFACES =
        " SELECT " + UUID + ", " + NODE_NAME + ", " + NODE_NET_NAME + ", " + NODE_NET_DSP_NAME + ", " +
                     INET_ADDRESS +
        " FROM " + TBL_NODE_NET_INTERFACES;

    private final NetInterfaceName niName;
    private final NodeName nodeName;

    private final String niAddrStr = "127.0.0.1";
    private final LsIpAddress niAddr;

    private Node node;

    private NetInterfaceDataDerbyDriver dbDriver;

    private java.util.UUID niUuid;
    private NetInterfaceData niData;
    private SingleColumnDatabaseDriver<NetInterfaceData, LsIpAddress> niAddrDriver;

    public NetInterfaceDataDerbyTest() throws Exception
    {
        nodeName = new NodeName("TestNodeName");
        niName = new NetInterfaceName("TestNetInterfaceName");
        niAddr = new LsIpAddress(niAddrStr);
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

        node = nodeDataFactory.getInstance(
            SYS_CTX,
            nodeName,
            null, // types
            null, // flags
            true,
            false
        );

        dbDriver = new NetInterfaceDataDerbyDriver(SYS_CTX, errorReporter, transObjFactory, transMgrProvider);
        niAddrDriver = dbDriver.getNetInterfaceAddressDriver();

        niUuid = java.util.UUID.randomUUID();

        // not persisted
        niData = new NetInterfaceData(
            niUuid, SYS_CTX, niName, node, niAddr, dbDriver, transObjFactory, transMgrProvider
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
        netInterfaceDataFactory.getInstance(
            SYS_CTX,
            node,
            netInterfaceName,
            niAddr,
            true,
            false
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
    public void testLoadSimple() throws Exception
    {
        dbDriver.create(niData);

        NetInterfaceData netData = dbDriver.load(node, niName, true);

        assertNotNull(netData);
        assertEquals(niUuid, netData.getUuid());
        assertEquals(nodeName.value, netData.getNode().getName().value);
        assertEquals(niName.value, netData.getName().value);
        assertEquals(niName.displayValue, netData.getName().displayValue);
        assertEquals(niAddrStr, netData.getAddress(SYS_CTX).getAddress());
    }

    @Test
    public void testLoadRestore() throws Exception
    {
        dbDriver.create(niData);

        NetInterfaceData netData = dbDriver.load(node, niName, true);

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

        NetInterfaceData netData1 = netInterfaceDataFactory.getInstance(
            SYS_CTX,
            node,
            niName,
            niAddr,
            false,
            false
        );

        assertNotNull(netData1);
        assertEquals(niUuid, netData1.getUuid());
        assertEquals(nodeName.value, netData1.getNode().getName().value);
        assertEquals(niName.value, netData1.getName().value);
        assertEquals(niName.displayValue, netData1.getName().displayValue);
        assertEquals(niAddrStr, netData1.getAddress(SYS_CTX).getAddress());

        NetInterfaceData netData2 = netInterfaceDataFactory.getInstance(
            SYS_CTX,
            node,
            niName,
            niAddr,
            false,
            false
        );
        assertTrue(netData1 == netData2);
    }

    @Test
    public void testLoadAll() throws Exception
    {
        dbDriver.create(niData);

        List<NetInterfaceData> niList = dbDriver.loadNetInterfaceData(node);
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
        NetInterfaceData storedInstance = netInterfaceDataFactory.getInstance(
            SYS_CTX,
            node,
            niName,
            niAddr,
            true,
            false
        );

        // no clearCaches
        assertEquals(storedInstance, dbDriver.load(node, niName, true));
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

    @Test
    public void testHalfValidName() throws Exception
    {
        dbDriver.create(niData);

        NetInterfaceName halfValidNiName = new NetInterfaceName(niData.getName().value);

        NetInterfaceData loadedNi = dbDriver.load(node, halfValidNiName, true);

        assertNotNull(loadedNi);
        assertEquals(niData.getName(), loadedNi.getName());
        assertEquals(niData.getUuid(), loadedNi.getUuid());
    }

    @Test (expected = LinStorDataAlreadyExistsException.class)
    public void testAlreadyExists() throws Exception
    {
        dbDriver.create(niData);

        netInterfaceDataFactory.getInstance(
            SYS_CTX,
            node,
            niName,
            niAddr,
            false,
            true
        );
    }
}
