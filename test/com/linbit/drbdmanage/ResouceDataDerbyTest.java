package com.linbit.drbdmanage;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import org.junit.Before;
import org.junit.Test;

import com.linbit.InvalidNameException;
import com.linbit.TransactionMgr;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbdmanage.Resource.RscFlags;
import com.linbit.drbdmanage.security.DerbyBase;
import com.linbit.drbdmanage.security.ObjectProtection;
import com.linbit.drbdmanage.stateflags.StateFlagsBits;
import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;
import com.linbit.utils.UuidUtils;

public class ResouceDataDerbyTest extends DerbyBase
{
    private static final String SELECT_ALL_RESOURCES =
        " SELECT " + UUID + ", " + NODE_NAME + ", " + RESOURCE_NAME + ", " + NODE_ID + ", " + RES_FLAGS +
        " FROM " + TBL_NODE_RESOURCE;
    private static final String SELECT_ALL_VOLUMES =
        " SELEFT " + UUID + ", " + NODE_NAME + ", " + RESOURCE_NAME + ", " + VLM_NR + ", " +
                     BLOCK_DEVICE_PATH + ", " + VLM_FLAGS +
        " FROM " + TBL_VOLUMES;

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

    private ResourceDataDerbyDriver driver;

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
        driver = (ResourceDataDerbyDriver) DrbdManage.getResourceDataDatabaseDriver(resName);
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
        driver.create(con, res);
        driver.delete(con, res);

        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_RESOURCES);
        ResultSet resultSet = stmt.executeQuery();

        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testStateFlagPersistence() throws Exception
    {
        driver.create(con, res);
        StateFlagsPersistence stateFlagPersistence = driver.getStateFlagPersistence(nodeName);
        stateFlagPersistence.persist(con, StateFlagsBits.getMask(RscFlags.REMOVE));

        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_RESOURCES);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        assertEquals(RscFlags.REMOVE.flagValue, resultSet.getLong(RES_FLAGS));

        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

//    @Test
//    public void testVolumeMapDriverInsert() throws Exception
//    {
//        driver.create(con, res);
//        MapDatabaseDriver<VolumeNumber, Volume> volumeMapDriver = driver.getVolumeMapDriver();
//
//        VolumeNumber volNr = new VolumeNumber(13);
//        MinorNumber minor = new MinorNumber(42);
//        long volSize = 9001;
//        Set<VlmDfnFlags> initFlags = null;
//        VolumeDefinitionData volDfnRef = VolumeDefinitionData.getInstance(
//            resDfn,
//            volNr,
//            transMgr,
//            null,
//            sysCtx,
//            minor ,
//            volSize ,
//            initFlags ,
//            true
//        );
//
//        java.util.UUID volUuid = randomUUID();
//        String volBlock = "testBlock";
//        VolumeData vol = new VolumeData(volUuid, res, volDfnRef, volBlock, null);
//
//        volumeMapDriver.insert(con, volNr, vol);
//
//        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_VOLUMES);
//        ResultSet resultSet = stmt.executeQuery();
//
//        assertTrue(resultSet.next());
//        assertEquals(volUuid, UuidUtils.asUUID(resultSet.getBytes(UUID)));
//        assertEquals(nodeName.value, resultSet.getString(NODE_NAME));
//        assertEquals(resName.value, resultSet.getString(RESOURCE_NAME));
//        assertEquals(volNr.value, resultSet.getInt(VLM_NR));
//        assertEquals(volBlock, resultSet.getString(BLOCK_DEVICE_PATH));
//        assertEquals(0, resultSet.getLong(VLM_FLAGS));
//
//        assertFalse(resultSet.next());
//
//        resultSet.close();
//        stmt.close();
//    }
//
//    @Test
//    public void testVolumeMapDriverUpdate() throws Exception
//    {
//        driver.create(con, res);
//        MapDatabaseDriver<VolumeNumber, Volume> volumeMapDriver = driver.getVolumeMapDriver();
//
//        VolumeNumber volNr = new VolumeNumber(13);
//        MinorNumber minor = new MinorNumber(42);
//        long volSize = 9001;
//        Set<VlmDfnFlags> initFlags = null;
//        VolumeDefinitionData volDfnRef = VolumeDefinitionData.getInstance(
//            resDfn,
//            volNr,
//            transMgr,
//            null,
//            sysCtx,
//            minor ,
//            volSize ,
//            initFlags ,
//            true
//        );
//
//        java.util.UUID volUuid = randomUUID();
//        String volBlock = "testBlock";
//        VolumeData vol = new VolumeData(volUuid, res, volDfnRef, volBlock, null);
//
//        volumeMapDriver.insert(con, volNr, vol);
//
//        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_VOLUMES);
//        ResultSet resultSet = stmt.executeQuery();
//
//        assertTrue(resultSet.next());
//        assertEquals(volUuid, UuidUtils.asUUID(resultSet.getBytes(UUID)));
//        assertEquals(nodeName.value, resultSet.getString(NODE_NAME));
//        assertEquals(resName.value, resultSet.getString(RESOURCE_NAME));
//        assertEquals(volNr.value, resultSet.getInt(VLM_NR));
//        assertEquals(volBlock, resultSet.getString(BLOCK_DEVICE_PATH));
//        assertEquals(0, resultSet.getLong(VLM_FLAGS));
//
//        assertFalse(resultSet.next());
//
//        resultSet.close();
//        stmt.close();
//    }
//
//    @Test
//    public void testVolumeMapDriverDelete() throws Exception
//    {
//        MapDatabaseDriver<VolumeNumber, Volume> volumeMapDriver = driver.getVolumeMapDriver();
//        fail("Test not implemented yet");
//    }

}
