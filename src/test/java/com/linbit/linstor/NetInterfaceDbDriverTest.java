package com.linbit.linstor;

import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.identifier.NetInterfaceName;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.NetInterface;
import com.linbit.linstor.core.objects.NetInterface.EncryptionType;
import com.linbit.linstor.core.objects.NetInterfaceDbDriver;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.TestFactory;
import com.linbit.linstor.core.types.LsIpAddress;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.security.GenericDbBase;

import javax.inject.Inject;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
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

public class NetInterfaceDbDriverTest extends GenericDbBase
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

    @Inject
    private NetInterfaceDbDriver dbDriver;

    private java.util.UUID niUuid;
    private NetInterface niData;
    private SingleColumnDatabaseDriver<NetInterface, LsIpAddress> niAddrDriver;

    public NetInterfaceDbDriverTest() throws Exception
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

        node = nodeFactory.create(
            SYS_CTX,
            nodeName,
            null, // types
            null // flags
        );

        niAddrDriver = dbDriver.getNetInterfaceAddressDriver();

        niUuid = java.util.UUID.randomUUID();

        // not persisted
        niData = TestFactory.createNetInterface(
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
        catch (DatabaseException exc)
        {
            // expected
        }
    }

    @Test
    public void testPersistGetInstance() throws Exception
    {
        NetInterfaceName netInterfaceName = new NetInterfaceName("TestNetIface");
        netInterfaceFactory.create(
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
        List<NetInterface> niList = dbDriver.loadAllAsList(tmpNodesMap);
        assertNotNull(niList);
        assertEquals(1, niList.size());

        NetInterface netData = niList.get(0);

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
        node.addNetInterface(SYS_CTX, niData);

        NetInterface netData1 = node.getNetInterface(SYS_CTX, niName);

        assertNotNull(netData1);
        assertEquals(niUuid, netData1.getUuid());
        assertEquals(nodeName.value, netData1.getNode().getName().value);
        assertEquals(niName.value, netData1.getName().value);
        assertEquals(niName.displayValue, netData1.getName().displayValue);
        assertEquals(niAddrStr, netData1.getAddress(SYS_CTX).getAddress());

        NetInterface netData2 = node.getNetInterface(SYS_CTX, niName);
        assertTrue(netData1 == netData2);
    }

    @Test
    public void testLoadAll() throws Exception
    {
        dbDriver.create(niData);
        nodesMap.put(nodeName, node);

        List<NetInterface> niList = dbDriver.loadAllAsList(nodesMap);
        assertEquals(1, niList.size());
        NetInterface netData = niList.get(0);

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
        NetInterface storedInstance = netInterfaceFactory.create(
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
        niData.setAddress(SYS_CTX, addr);
        // niAddrDriver.update(niData, addr);
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
        node.addNetInterface(SYS_CTX, niData);

        netInterfaceFactory.create(
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
