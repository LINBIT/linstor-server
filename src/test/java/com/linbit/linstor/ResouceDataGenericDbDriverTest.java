package com.linbit.linstor;

import javax.inject.Inject;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.Resource.InitMaps;
import com.linbit.linstor.Resource.RscFlags;
import com.linbit.linstor.ResourceDefinition.TransportType;
import com.linbit.linstor.security.GenericDbBase;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.stateflags.StateFlagsBits;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import org.junit.Before;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ResouceDataGenericDbDriverTest extends GenericDbBase
{
    private static final String SELECT_ALL_RESOURCES =
        " SELECT " + UUID + ", " + NODE_NAME + ", " + RESOURCE_NAME + ", " + NODE_ID + ", " + RESOURCE_FLAGS +
        " FROM " + TBL_RESOURCES;

    private final NodeName nodeName;
    private final ResourceName resName;
    private final Integer resPort;
    private final NodeId nodeId;

    private NodeData node;
    private ResourceDefinitionData resDfn;

    private java.util.UUID resUuid;
    private ObjectProtection objProt;
    private long initFlags;

    @Inject private ResourceDataGenericDbDriver driver;

    @SuppressWarnings("checkstyle:magicnumber")
    public ResouceDataGenericDbDriverTest() throws InvalidNameException, ValueOutOfRangeException
    {
        nodeName = new NodeName("TestNodeName");
        resName = new ResourceName("TestResName");
        resPort = 9001;
        nodeId = new NodeId(13);
    }

    @SuppressWarnings("checkstyle:magicnumber")
    @Before
    public void setUp() throws Exception
    {
        super.setUpAndEnterScope();
        assertEquals(
            TBL_RESOURCES + " table's column count has changed. Update tests accordingly!",
            5,
            TBL_COL_COUNT_RESOURCES
        );

        node = nodeDataFactory.create(SYS_CTX, nodeName, null, null);
        resDfn = resourceDefinitionDataFactory.create(
            SYS_CTX, resName, resPort, null, "secret", TransportType.IP
        );

        resUuid = randomUUID();
        objProt = objectProtectionFactory.getInstance(SYS_CTX, ObjectProtection.buildPath(nodeName, resName), true);

        initFlags = RscFlags.CLEAN.flagValue;
    }

    @Test
    public void testPersist() throws Exception
    {
        ResourceData res = new ResourceData(
            resUuid,
            objProt,
            resDfn,
            node,
            nodeId,
            initFlags,
            driver,
            propsContainerFactory,
            volumeDataFactory,
            transObjFactory,
            transMgrProvider,
            new TreeMap<>(),
            new TreeMap<>()
        );
        driver.create(res);
        commit();

        PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_RESOURCES);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue("Database did not persist resource / resourceDefinition", resultSet.next());
        assertEquals(resUuid, java.util.UUID.fromString(resultSet.getString(UUID)));
        assertEquals(nodeName.value, resultSet.getString(NODE_NAME));
        assertEquals(resName.value, resultSet.getString(RESOURCE_NAME));
        assertEquals(nodeId.value, resultSet.getInt(NODE_ID));
        assertEquals(RscFlags.CLEAN.flagValue, resultSet.getLong(RESOURCE_FLAGS));
        assertFalse("Database persisted too many resources / resourceDefinitions", resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testPersistGetInstance() throws Exception
    {
        resourceDataFactory.create(
            SYS_CTX,
            resDfn,
            node,
            nodeId,
            new RscFlags[] {RscFlags.DELETE}
        );

        commit();

        PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_RESOURCES);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue("Database did not persist resource / resourceDefinition", resultSet.next());
        // uuid is now != resUuid because getInstance create a new resData object
        assertEquals(nodeName.value, resultSet.getString(NODE_NAME));
        assertEquals(resName.value, resultSet.getString(RESOURCE_NAME));
        assertEquals(nodeId.value, resultSet.getInt(NODE_ID));
        assertEquals(RscFlags.DELETE.flagValue, resultSet.getLong(RESOURCE_FLAGS));
        assertFalse("Database persisted too many resources / resourceDefinitions", resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testLoadGetInstance() throws Exception
    {
        ResourceData loadedRes = (ResourceData) node.getResource(SYS_CTX, resDfn.getName());
        assertNull(loadedRes);

        ResourceData res = new ResourceData(
            resUuid,
            objProt,
            resDfn,
            node,
            nodeId,
            initFlags,
            driver,
            propsContainerFactory,
            volumeDataFactory,
            transObjFactory,
            transMgrProvider,
            new TreeMap<>(),
            new TreeMap<>()
        );
        driver.create(res);
        node.addResource(SYS_CTX, res);
        resDfn.addResource(SYS_CTX, res);

        loadedRes = (ResourceData) node.getResource(SYS_CTX, resDfn.getName());

        assertNotNull("Database did not persist resource / resourceDefinition", loadedRes);
        assertEquals(resUuid, loadedRes.getUuid());
        assertNotNull(loadedRes.getAssignedNode());
        assertEquals(nodeName, loadedRes.getAssignedNode().getName());
        assertNotNull(loadedRes.getDefinition());
        assertEquals(resName, loadedRes.getDefinition().getName());
        assertEquals(nodeId, loadedRes.getNodeId());
        assertEquals(RscFlags.CLEAN.flagValue, loadedRes.getStateFlags().getFlagsBits(SYS_CTX));
    }

    @Test
    public void testLoadAll() throws Exception
    {
        ResourceData res = new ResourceData(
            resUuid,
            objProt,
            resDfn,
            node,
            nodeId,
            initFlags,
            driver,
            propsContainerFactory,
            volumeDataFactory,
            transObjFactory,
            transMgrProvider,
            new TreeMap<>(),
            new TreeMap<>()
        );
        driver.create(res);

        nodesMap.put(nodeName, node);
        rscDfnMap.put(resName, resDfn);

        Map<ResourceData, InitMaps> resList = driver.loadAll(nodesMap, rscDfnMap);

        assertNotNull(resList);
        assertEquals(1, resList.size());

        ResourceData resData = resList.keySet().iterator().next();

        assertNotNull(resData);
        assertEquals(resUuid, resData.getUuid());
        assertNotNull(resData.getAssignedNode());
        assertEquals(nodeName, resData.getAssignedNode().getName());
        assertNotNull(resData.getDefinition());
        assertEquals(resName, resData.getDefinition().getName());
        assertEquals(nodeId, resData.getNodeId());
        assertEquals(RscFlags.CLEAN.flagValue, resData.getStateFlags().getFlagsBits(SYS_CTX));
    }

    @Test
    public void testCache() throws Exception
    {
        ResourceData storedInstance = resourceDataFactory.create(
            SYS_CTX,
            resDfn,
            node,
            nodeId,
            null
        );

        // no clearCaches

        assertEquals(storedInstance, node.getResource(SYS_CTX, resDfn.getName()));
    }

    @Test
    public void testDelete() throws Exception
    {
        ResourceData res = new ResourceData(
            resUuid,
            objProt,
            resDfn,
            node,
            nodeId,
            initFlags,
            driver,
            propsContainerFactory,
            volumeDataFactory,
            transObjFactory,
            transMgrProvider,
            new TreeMap<>(),
            new TreeMap<>()
        );
        driver.create(res);
        commit();
        driver.delete(res);
        commit();

        PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_RESOURCES);
        ResultSet resultSet = stmt.executeQuery();

        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testStateFlagPersistence() throws Exception
    {
        ResourceData res = new ResourceData(
            resUuid,
            objProt,
            resDfn,
            node,
            nodeId,
            initFlags,
            driver,
            propsContainerFactory,
            volumeDataFactory,
            transObjFactory,
            transMgrProvider,
            new TreeMap<>(),
            new TreeMap<>()
        );
        driver.create(res);
        commit();
        StateFlagsPersistence<ResourceData> stateFlagPersistence = driver.getStateFlagPersistence();
        stateFlagPersistence.persist(res, StateFlagsBits.getMask(RscFlags.DELETE));
        commit();

        PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_RESOURCES);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        assertEquals(RscFlags.DELETE.flagValue, resultSet.getLong(RESOURCE_FLAGS));

        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testEnsureExists() throws Exception
    {
        PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_RESOURCES);
        ResultSet resultSet = stmt.executeQuery();

        assertFalse(resultSet.next());
        resultSet.close();

        ResourceData res = new ResourceData(
            resUuid,
            objProt,
            resDfn,
            node,
            nodeId,
            initFlags,
            driver,
            propsContainerFactory,
            volumeDataFactory,
            transObjFactory,
            transMgrProvider,
            new TreeMap<>(),
            new TreeMap<>()
        );
        driver.ensureResExists(SYS_CTX, res);
        commit();

        resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        assertFalse(resultSet.next());
        resultSet.close();

        driver.ensureResExists(SYS_CTX, res);
        commit();

        resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        assertFalse(resultSet.next());
        resultSet.close();
        stmt.close();
    }

    @Test (expected = LinStorDataAlreadyExistsException.class)
    public void testAlreadyExists() throws Exception
    {
        ResourceData res = new ResourceData(
            resUuid,
            objProt,
            resDfn,
            node,
            nodeId,
            initFlags,
            driver,
            propsContainerFactory,
            volumeDataFactory,
            transObjFactory,
            transMgrProvider,
            new TreeMap<>(),
            new TreeMap<>()
        );
        driver.create(res);
        node.addResource(SYS_CTX, res);
        resDfn.addResource(SYS_CTX, res);

        resourceDataFactory.create(SYS_CTX, resDfn, node, nodeId, null);
    }
}
