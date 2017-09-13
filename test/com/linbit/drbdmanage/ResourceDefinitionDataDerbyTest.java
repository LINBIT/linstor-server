package com.linbit.drbdmanage;

import static org.junit.Assert.*;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

import com.linbit.InvalidNameException;
import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.Resource.RscFlags;
import com.linbit.drbdmanage.ResourceDefinition.RscDfnFlags;
import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.propscon.PropsContainer;
import com.linbit.drbdmanage.propscon.SerialGenerator;
import com.linbit.drbdmanage.security.DerbyBase;
import com.linbit.drbdmanage.security.ObjectProtection;
import com.linbit.utils.UuidUtils;

public class ResourceDefinitionDataDerbyTest extends DerbyBase
{
    private static final String SELECT_ALL_RESOURCE_DEFINITIONS =
        " SELECT " + UUID + ", " + RESOURCE_NAME + ", " +
                     RESOURCE_DSP_NAME + ", " + RESOURCE_FLAGS +
        " FROM " + TBL_RESOURCE_DEFINITIONS;

    private final ResourceName resName;
    private final int conNr;
    private final NodeName conNodeName1;
    private final NodeName conNodeName2;

    private TransactionMgr transMgr;
    private java.util.UUID resDfnUuid;
    private ObjectProtection resDfnObjProt;

    private NodeId node1Id;
    private Node node1;
    private Node node2;

    private ResourceDefinitionData resDfn;
    private ResourceDefinitionDataDerbyDriver driver;

    public ResourceDefinitionDataDerbyTest() throws InvalidNameException
    {
        resName = new ResourceName("TestResName");
        conNr = 42;
        conNodeName1 = new NodeName("TestNodeName1");
        conNodeName2 = new NodeName("TestNodeName2");
    }

    @Before
    public void startUp() throws Exception
    {
        assertEquals(TBL_RESOURCE_DEFINITIONS + " table's column count has changed. Update tests accordingly!", 4, TBL_COL_COUNT_RESOURCE_DEFINITIONS);

        transMgr = new TransactionMgr(getConnection());

        resDfnUuid = randomUUID();

        resDfnObjProt = ObjectProtection.getInstance(
            sysCtx,
            ObjectProtection.buildPath(resName),
            true,
            transMgr
        );

        node1Id = new NodeId(1);
        node1 = NodeData.getInstance(
            sysCtx,
            conNodeName1,
            null,
            null,
            null,
            transMgr,
            true
        );
        node2 = NodeData.getInstance(
            sysCtx,
            conNodeName2,
            null,
            null,
            null,
            transMgr,
            true
        );
        resDfn = new ResourceDefinitionData(
            resDfnUuid,
            resDfnObjProt,
            resName,
            RscDfnFlags.REMOVE.flagValue,
            null,
            transMgr
        );


        driver = new ResourceDefinitionDataDerbyDriver(sysCtx, errorReporter);
    }

    @Test
    public void testPersist() throws Exception
    {
        driver.create(resDfn, transMgr);

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_RESOURCE_DEFINITIONS);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue("Database did not persist resourceDefinition", resultSet.next());
        assertEquals(resDfnUuid, UuidUtils.asUuid(resultSet.getBytes(UUID)));
        assertEquals(resName.value, resultSet.getString(RESOURCE_NAME));
        assertEquals(resName.displayValue, resultSet.getString(RESOURCE_DSP_NAME));
        assertEquals(RscDfnFlags.REMOVE.flagValue, resultSet.getLong(RESOURCE_FLAGS));
        assertFalse("Database persisted too many resourceDefinitions", resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testPersistGetInstance() throws Exception
    {
        ResourceDefinitionData.getInstance(
            sysCtx,
            resName,
            new RscDfnFlags[] { RscDfnFlags.REMOVE },
            null,
            transMgr,
            true
        );

        transMgr.commit();

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_RESOURCE_DEFINITIONS);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue("Database did not persist resource / resourceDefinition", resultSet.next());
        // uuid is now != resUuid because getInstance create a new resData object
        assertEquals(resName.value, resultSet.getString(RESOURCE_NAME));
        assertEquals(resName.displayValue, resultSet.getString(RESOURCE_DSP_NAME));
        assertEquals(RscDfnFlags.REMOVE.flagValue, resultSet.getLong(RESOURCE_FLAGS));
        assertFalse("Database persisted too many resources / resourceDefinitions", resultSet.next());

        resultSet.close();
        stmt.close();

    }

    @Test
    public void testLoad() throws Exception
    {
        driver.create(resDfn, transMgr);

        ResourceData.getInstance(
            sysCtx,
            resDfn,
            node1,
            node1Id,
            null,
            null,
            transMgr,
            true
        );

        ConnectionDefinitionData.getInstance(
            sysCtx,
            resDfn,
            node1,
            node2,
            conNr,
            null,
            transMgr,
            true
        );

        DriverUtils.clearCaches();

        ResourceDefinitionData loadedResDfn = driver.load(resName, null, transMgr);

        assertNotNull("Database did not persist resource / resourceDefinition", loadedResDfn);
        assertEquals(resDfnUuid, loadedResDfn.getUuid());
        assertEquals(resName, loadedResDfn.getName());
        assertEquals(RscDfnFlags.REMOVE.flagValue, loadedResDfn.getFlags().getFlagsBits(sysCtx));

        ConnectionDefinition loadedConDfn = loadedResDfn.getConnectionDfn(sysCtx, conNodeName1, conNr);
        assertNotNull(loadedConDfn);
        if (loadedConDfn.getSourceNode(sysCtx).getName().equals(conNodeName1))
        {
            assertEquals(conNodeName2, loadedConDfn.getTargetNode(sysCtx).getName());
        }
        else
        {
            assertEquals(conNodeName2, loadedConDfn.getSourceNode(sysCtx).getName());
        }
    }

    @Test
    public void testLoadGetInstance() throws Exception
    {
        ResourceDefinitionData loadedResDfn = ResourceDefinitionData.getInstance(
            sysCtx,
            resName,
            new RscDfnFlags[] { RscDfnFlags.REMOVE },
            null,
            transMgr,
            false
        );

        assertNull(loadedResDfn);

        driver.create(resDfn, transMgr);

        ResourceData.getInstance(
            sysCtx,
            resDfn,
            node1,
            node1Id,
            null,
            null,
            transMgr,
            true
        );

        ConnectionDefinitionData.getInstance(
            sysCtx,
            resDfn,
            node1,
            node2,
            conNr,
            null,
            transMgr,
            true
        );

        DriverUtils.clearCaches();

        loadedResDfn = ResourceDefinitionData.getInstance(
            sysCtx,
            resName,
            new RscDfnFlags[] { RscDfnFlags.REMOVE },
            null,
            transMgr,
            false
        );

        assertNotNull("Database did not persist resource / resourceDefinition", loadedResDfn);
        assertEquals(resDfnUuid, loadedResDfn.getUuid());
        assertEquals(resName, loadedResDfn.getName());
        assertEquals(RscDfnFlags.REMOVE.flagValue, loadedResDfn.getFlags().getFlagsBits(sysCtx));

        ConnectionDefinition loadedConDfn = loadedResDfn.getConnectionDfn(sysCtx, conNodeName1, conNr);
        assertNotNull(loadedConDfn);
        if (loadedConDfn.getSourceNode(sysCtx).getName().equals(conNodeName1))
        {
            assertEquals(conNodeName2, loadedConDfn.getTargetNode(sysCtx).getName());
        }
        else
        {
            assertEquals(conNodeName2, loadedConDfn.getSourceNode(sysCtx).getName());
        }
    }

    @Test
    public void testCache() throws Exception
    {
        driver.create(resDfn, transMgr);

        // no clearCaches

        assertEquals(resDfn, driver.load(resName, null, transMgr));
    }

    @Test
    public void testDelete() throws Exception
    {
        driver.create(resDfn, transMgr);

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_RESOURCE_DEFINITIONS);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue("Database did not persist resourceDefinition", resultSet.next());
        resultSet.close();

        driver.delete(resDfn, transMgr);

        resultSet = stmt.executeQuery();
        assertFalse("Database did not delete resourceDefinition", resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testPersistProps() throws Exception
    {
        resDfn.initialized();
        resDfn.setConnection(transMgr);
        driver.create(resDfn, transMgr);

        Props props = resDfn.getProps(sysCtx);
        String testKey = "TestKey";
        String testValue = "TestValue";
        props.setProp(testKey, testValue);

        transMgr.commit();

        Map<String, String> testMap = new HashMap<>();
        testMap.put(testKey, testValue);
        testProps(transMgr, PropsContainer.buildPath(resName), testMap, true);
    }

    @Test
    public void testLoadProps() throws Exception
    {
        driver.create(resDfn, transMgr);
        String testKey = "TestKey";
        String testValue = "TestValue";
        insertProp(transMgr, PropsContainer.buildPath(resName), testKey, testValue);

        DriverUtils.clearCaches();

        ResourceDefinitionData loadedResDfn = driver.load(resName, null, transMgr);

        Props props = loadedResDfn.getProps(sysCtx);

        assertNotNull(props);
        assertEquals(testValue, props.getProp(testKey));
        assertNotNull(props.getProp(SerialGenerator.KEY_SERIAL));
        assertEquals(2, props.size());
    }

    @Test
    public void testLoadResources() throws Exception
    {
        driver.create(resDfn, transMgr);
        NodeName nodeName = new NodeName("TestNodeName");
        Node node = NodeData.getInstance(sysCtx, nodeName, null, null, null, transMgr, true);
        NodeId nodeId = new NodeId(13);
        ResourceData res = ResourceData.getInstance(
            sysCtx,
            resDfn,
            node,
            nodeId,
            new RscFlags[] { RscFlags.CLEAN },
            null,
            transMgr,
            true
        );

        DriverUtils.clearCaches();

        ResourceDefinitionData loadedResDfn = driver.load(resName, null, transMgr);
        Resource loadedRes = loadedResDfn.getResource(sysCtx, nodeName);

        assertNotNull(loadedRes);
        assertEquals(nodeName, loadedRes.getAssignedNode().getName());
        assertEquals(loadedResDfn, loadedRes.getDefinition());
        assertEquals(nodeId, loadedRes.getNodeId());
        assertNotNull(loadedRes.getObjProt());
        assertNotNull(loadedRes.getProps(sysCtx));
        assertEquals(RscFlags.CLEAN.flagValue, loadedRes.getStateFlags().getFlagsBits(sysCtx));
        assertEquals(res.getUuid(), loadedRes.getUuid());
    }

    @Test
    public void testLoadVolumeDefinitions() throws Exception
    {
        driver.create(resDfn, transMgr);

        VolumeNumber volNr = new VolumeNumber(13);
        MinorNumber minor = new MinorNumber(42);
        long volSize = 5_000;
        VolumeDefinitionData volDfn = VolumeDefinitionData.getInstance(sysCtx, resDfn, volNr, minor, volSize, null, null, transMgr, true);

        DriverUtils.clearCaches();

        ResourceDefinitionData loadedResDfn = driver.load(resName, null, transMgr);
        VolumeDefinition loadedVolDfn = loadedResDfn.getVolumeDfn(sysCtx, volNr);

        assertNotNull(loadedVolDfn);
        assertEquals(volDfn.getUuid(), loadedVolDfn.getUuid());
        assertEquals(volDfn.getFlags().getFlagsBits(sysCtx), loadedVolDfn.getFlags().getFlagsBits(sysCtx));
        assertEquals(minor, loadedVolDfn.getMinorNr(sysCtx));
        assertEquals(volNr, loadedVolDfn.getVolumeNumber(sysCtx));
        assertEquals(volSize, loadedVolDfn.getVolumeSize(sysCtx));
        assertEquals(loadedResDfn, loadedVolDfn.getResourceDefinition());
    }

    @Test
    public void testStateFlagPersistence() throws Exception
    {
        driver.create(resDfn, transMgr);
        resDfn.initialized();
        resDfn.setConnection(transMgr);

        resDfn.getFlags().disableAllFlags(sysCtx);

        transMgr.commit();

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_RESOURCE_DEFINITIONS);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        assertEquals(0, resultSet.getLong(RESOURCE_FLAGS));

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testExists() throws Exception
    {
        assertFalse(driver.exists(resName, transMgr));
        driver.create(resDfn, transMgr);
        assertTrue(driver.exists(resName, transMgr));
    }

    @Test
    public void testGetInstanceSatelliteCreate() throws Exception
    {
        ResourceDefinitionData instance = ResourceDefinitionData.getInstance(
            sysCtx,
            resName,
            new RscDfnFlags[] { RscDfnFlags.REMOVE },
            null,
            null,
            true
        );

        assertNotNull(instance);
        assertEquals(resName, instance.getName());

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_RESOURCE_DEFINITIONS);
        ResultSet resultSet = stmt.executeQuery();

        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testGetInstanceSatelliteNoCreate() throws Exception
    {
        ResourceDefinitionData instance = ResourceDefinitionData.getInstance(
            sysCtx,
            resName,
            new RscDfnFlags[] { RscDfnFlags.REMOVE },
            null,
            null,
            false
        );

        assertNull(instance);

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_RESOURCE_DEFINITIONS);
        ResultSet resultSet = stmt.executeQuery();

        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testHalfValidName() throws Exception
    {
        driver.create(resDfn, transMgr);
        DriverUtils.clearCaches();

        ResourceName halfValidResName = new ResourceName(resDfn.getName().value);

        ResourceDefinitionData loadedResDfn = driver.load(halfValidResName, null, transMgr);

        assertNotNull(loadedResDfn);
        assertEquals(resDfn.getName(), loadedResDfn.getName());
        assertEquals(resDfn.getUuid(), loadedResDfn.getUuid());
    }
}
