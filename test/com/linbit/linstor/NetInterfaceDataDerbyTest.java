package com.linbit.linstor;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.TransactionMgr;
import com.linbit.linstor.security.DerbyBase;
import com.linbit.utils.UuidUtils;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
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

    private TransactionMgr transMgr;

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

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        assertEquals(TBL_NODE_NET_INTERFACES + " table's column count has changed. Update tests accordingly!", 5, TBL_COL_COUNT_NODE_NET_INTERFACES);

        transMgr = new TransactionMgr(getConnection());

        node = nodeDataFactory.getInstance(
            SYS_CTX,
            nodeName,
            null, // types
            null, // flags
            transMgr,
            true,
            false
        );

        dbDriver = new NetInterfaceDataDerbyDriver(SYS_CTX, errorReporter);
        niAddrDriver = dbDriver.getNetInterfaceAddressDriver();

        niUuid = java.util.UUID.randomUUID();
        niData = new NetInterfaceData(niUuid, SYS_CTX, niName, node, niAddr, dbDriver); // does not persist
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
        netInterfaceDataFactory.getInstance(
            SYS_CTX,
            node,
            netInterfaceName,
            niAddr,
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
        assertEquals(niAddrStr, netData.getAddress(SYS_CTX).getAddress());
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
        assertEquals(niAddrStr, netData.getAddress(SYS_CTX).getAddress());
    }

    @Test
    public void testLoadGetInstanceTwice() throws Exception
    {
        niData.initialized();
        dbDriver.create(niData, transMgr);

        NetInterfaceData netData1 = netInterfaceDataFactory.getInstance(
            SYS_CTX,
            node,
            niName,
            niAddr,
            transMgr,
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
        niData.setAddress(SYS_CTX, addr);
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

        netInterfaceDataFactory.getInstance(
            SYS_CTX,
            node,
            niName,
            niAddr,
            transMgr,
            false,
            true
        );
    }
}
