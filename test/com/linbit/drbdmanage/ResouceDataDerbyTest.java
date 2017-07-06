package com.linbit.drbdmanage;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.junit.Before;
import org.junit.Test;

import com.linbit.InvalidNameException;
import com.linbit.MapDatabaseDriver;
import com.linbit.TransactionMgr;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbdmanage.dbdrivers.interfaces.ResourceDataDatabaseDriver;
import com.linbit.drbdmanage.security.DerbyBase;
import com.linbit.drbdmanage.security.ObjectProtection;
import com.linbit.utils.UuidUtils;

public class ResouceDataDerbyTest extends DerbyBase
{
    private static final String SELECT_ALL_RESOURCES =
        " SELECT " + UUID + ", " + NODE_NAME + ", " + RESOURCE_NAME + ", " + NODE_ID + ", " + RES_FLAGS +
        " FROM " + TBL_NODE_RESOURCE;

    private final NodeName nodeName;
    private final ResourceName resName;
    private final NodeId nodeId;

    private Connection con;
    private TransactionMgr transMgr;
    private NodeData node;
    private ResourceDefinitionData resDfn;

    private java.util.UUID resUuid;
    private ObjectProtection objProt;
    private ResourceData res;

    private ResourceDataDatabaseDriver driver;


    public ResouceDataDerbyTest() throws InvalidNameException, ValueOutOfRangeException
    {
        nodeName = new NodeName("TestNodeName");
        resName = new ResourceName("TestResName");
        nodeId = new NodeId(13);
    }

    @Before
    public void startUp() throws Exception
    {
        assertEquals(TBL_NODE_RESOURCE + " table's column count has changed. Update tests accordingly!", 5, TBL_COL_COUNT_NODE_RESOURCE);

        con = getConnection();
        transMgr = new TransactionMgr(con);

        node = NodeData.getInstance(sysCtx, nodeName, null, null, null, transMgr, true);
        resDfn = ResourceDefinitionData.getInstance(sysCtx, resName, null, transMgr, true);

        resUuid = randomUUID();
        objProt = ObjectProtection.getInstance(sysCtx, transMgr, ObjectProtection.buildPath(nodeName, resName), true);

        res = new ResourceData(resUuid, objProt, resDfn, node, nodeId, null, transMgr);
        driver = DrbdManage.getResourceDataDatabaseDriver(resName);
    }

    @Test
    public void testPersist() throws Exception
    {
        driver.create(con, res);

        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_RESOURCES);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue("Database did not persist resource / resourceDefinition", resultSet.next());
        assertEquals(resUuid, UuidUtils.asUUID(resultSet.getBytes(UUID)));
        assertEquals(nodeName.value, resultSet.getString(NODE_NAME));
        assertEquals(resName.value, resultSet.getString(RESOURCE_NAME));
        assertEquals(nodeId.value, resultSet.getInt(NODE_ID));
        assertEquals(0, resultSet.getLong(RES_FLAGS));
        assertFalse("Database persisted too many resources / resourceDefinitions", resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testPersistGetInstance() throws Exception
    {
        ResourceData.getInstance(sysCtx, resDfn, node, nodeId, null, transMgr, true);

        transMgr.commit();

        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_RESOURCES);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue("Database did not persist resource / resourceDefinition", resultSet.next());
        // uuid is now != resUuid because getInstance create a new resData object
        assertEquals(nodeName.value, resultSet.getString(NODE_NAME));
        assertEquals(resName.value, resultSet.getString(RESOURCE_NAME));
        assertEquals(nodeId.value, resultSet.getInt(NODE_ID));
        assertEquals(0, resultSet.getLong(RES_FLAGS));
        assertFalse("Database persisted too many resources / resourceDefinitions", resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testLoad() throws Exception
    {
        driver.create(con, res);

        ResourceData loadedRes = driver.load(con, node, null, transMgr);

        assertNotNull("Database did not persist resource / resourceDefinition", loadedRes);
        assertEquals(resUuid, loadedRes.getUuid());
        assertNotNull(loadedRes.getAssignedNode());
        assertEquals(nodeName, loadedRes.getAssignedNode().getName());
        assertNotNull(loadedRes.getDefinition());
        assertEquals(resName, loadedRes.getDefinition().getName());
        assertEquals(nodeId, loadedRes.getNodeId());
        assertEquals(0, loadedRes.getStateFlags().getFlagsBits(sysCtx));
    }

    @Test
    public void testLoadGetInstance() throws Exception
    {
        ResourceData loadedRes = ResourceData.getInstance(sysCtx, resDfn, node, nodeId, null, transMgr, false);
        assertNull(loadedRes);

        driver.create(con, res);

        loadedRes = ResourceData.getInstance(sysCtx, resDfn, node, nodeId, null, transMgr, false);

        assertNotNull("Database did not persist resource / resourceDefinition", loadedRes);
        assertEquals(resUuid, loadedRes.getUuid());
        assertNotNull(loadedRes.getAssignedNode());
        assertEquals(nodeName, loadedRes.getAssignedNode().getName());
        assertNotNull(loadedRes.getDefinition());
        assertEquals(resName, loadedRes.getDefinition().getName());
        assertEquals(nodeId, loadedRes.getNodeId());
        assertEquals(0, loadedRes.getStateFlags().getFlagsBits(sysCtx));
    }

    @Test
    public void testDelete() throws Exception
    {
        fail("Test not implemented yet");
    }

    @Test
    public void testStateFlagPersistence() throws Exception
    {
//        StateFlagsPersistence stateFlagPersistence = driver.getStateFlagPersistence();
//        stateFlagPersistence.persist(con, flags);
        fail("Test not implemented yet");
    }

    @Test
    public void testVolumeMapDriverInsert() throws Exception
    {
        MapDatabaseDriver<VolumeNumber, Volume> volumeMapDriver = driver.getVolumeMapDriver();
        fail("Test not implemented yet");
    }

    @Test
    public void testVolumeMapDriverUpdate() throws Exception
    {
        MapDatabaseDriver<VolumeNumber, Volume> volumeMapDriver = driver.getVolumeMapDriver();
        fail("Test not implemented yet");
    }

    @Test
    public void testVolumeMapDriverDelete() throws Exception
    {
        MapDatabaseDriver<VolumeNumber, Volume> volumeMapDriver = driver.getVolumeMapDriver();
        fail("Test not implemented yet");
    }

}
