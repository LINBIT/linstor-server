package com.linbit.linstor;

import com.google.inject.Inject;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.Resource.RscFlags;
import com.linbit.linstor.ResourceDefinition.TransportType;
import com.linbit.linstor.security.DerbyBase;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.stateflags.StateFlagsBits;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.utils.UuidUtils;
import org.junit.Before;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ResouceDataDerbyTest extends DerbyBase
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

    @Inject private ResourceDataDerbyDriver driver;

    @SuppressWarnings("checkstyle:magicnumber")
    public ResouceDataDerbyTest() throws InvalidNameException, ValueOutOfRangeException
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

        node = nodeDataFactory.getInstance(SYS_CTX, nodeName, null, null, true, false);
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
            SYS_CTX,
            objProt,
            resDfn,
            node,
            nodeId,
            initFlags,
            driver,
            propsContainerFactory,
            volumeDataFactory,
            transObjFactory,
            transMgrProvider
        );
        driver.create(res);
        commit();

        PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_RESOURCES);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue("Database did not persist resource / resourceDefinition", resultSet.next());
        assertEquals(resUuid, UuidUtils.asUuid(resultSet.getBytes(UUID)));
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
        resourceDataFactory.getInstance(
            SYS_CTX,
            resDfn,
            node,
            nodeId,
            new RscFlags[] { RscFlags.DELETE },
            true,
            false
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
    public void testLoad() throws Exception
    {
        ResourceData res = new ResourceData(
            resUuid,
            SYS_CTX,
            objProt,
            resDfn,
            node,
            nodeId,
            initFlags,
            driver,
            propsContainerFactory,
            volumeDataFactory,
            transObjFactory,
            transMgrProvider
        );
        driver.create(res);

        ResourceData loadedRes = driver.load(node, resName, true);

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
    public void testLoadGetInstance() throws Exception
    {
        ResourceData loadedRes = resourceDataFactory.getInstance(
            SYS_CTX,
            resDfn,
            node,
            nodeId,
            null,
            false,
            false
        );
        assertNull(loadedRes);

        ResourceData res = new ResourceData(
            resUuid,
            SYS_CTX,
            objProt,
            resDfn,
            node,
            nodeId,
            initFlags,
            driver,
            propsContainerFactory,
            volumeDataFactory,
            transObjFactory,
            transMgrProvider
        );
        driver.create(res);

        loadedRes = resourceDataFactory.getInstance(
            SYS_CTX,
            resDfn,
            node,
            nodeId,
            null,
            false,
            false
        );

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
            SYS_CTX,
            objProt,
            resDfn,
            node,
            nodeId,
            initFlags,
            driver,
            propsContainerFactory,
            volumeDataFactory,
            transObjFactory,
            transMgrProvider
        );
        driver.create(res);

        List<ResourceData> resList = driver.loadResourceData(SYS_CTX, node);

        assertNotNull(resList);
        assertEquals(1, resList.size());
        ResourceData resData = resList.get(0);
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
        ResourceData storedInstance = resourceDataFactory.getInstance(
            SYS_CTX,
            resDfn,
            node,
            nodeId,
            null,
            true,
            false
        );

        // no clearCaches

        assertEquals(storedInstance, driver.load(node, resName, true));
    }

    @Test
    public void testDelete() throws Exception
    {
        ResourceData res = new ResourceData(
            resUuid,
            SYS_CTX,
            objProt,
            resDfn,
            node,
            nodeId,
            initFlags,
            driver,
            propsContainerFactory,
            volumeDataFactory,
            transObjFactory,
            transMgrProvider
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
            SYS_CTX,
            objProt,
            resDfn,
            node,
            nodeId,
            initFlags,
            driver,
            propsContainerFactory,
            volumeDataFactory,
            transObjFactory,
            transMgrProvider
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
            SYS_CTX,
            objProt,
            resDfn,
            node,
            nodeId,
            initFlags,
            driver,
            propsContainerFactory,
            volumeDataFactory,
            transObjFactory,
            transMgrProvider
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
            SYS_CTX,
            objProt,
            resDfn,
            node,
            nodeId,
            initFlags,
            driver,
            propsContainerFactory,
            volumeDataFactory,
            transObjFactory,
            transMgrProvider
        );
        driver.create(res);

        resourceDataFactory.getInstance(SYS_CTX, resDfn, node, nodeId, null, false, true);
    }
}
