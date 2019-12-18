package com.linbit.linstor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGenericDbDriver;
import com.linbit.linstor.core.objects.TestFactory;
import com.linbit.linstor.security.GenericDbBase;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.stateflags.StateFlagsBits;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscDfnObject.TransportType;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;

import javax.inject.Inject;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import org.junit.Before;
import org.junit.Test;

public class ResourceGenericDbDriverTest extends GenericDbBase
{
    private static final String SELECT_ALL_RESOURCES =
        " SELECT " + UUID + ", " + NODE_NAME + ", " + RESOURCE_NAME + ", " + RESOURCE_FLAGS +
        " FROM " + TBL_RESOURCES;

    private final NodeName nodeName;
    private final ResourceName resName;
    private final Integer resPort;
    private final Integer nodeId;

    private Node node;
    private ResourceDefinition resDfn;

    private java.util.UUID resUuid;
    private ObjectProtection objProt;
    private long initFlags;

    @Inject private ResourceGenericDbDriver driver;

    @SuppressWarnings("checkstyle:magicnumber")
    public ResourceGenericDbDriverTest() throws InvalidNameException, ValueOutOfRangeException
    {
        nodeName = new NodeName("TestNodeName");
        resName = new ResourceName("TestResName");
        resPort = 9001;
        nodeId = 13;
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

        node = nodeFactory.create(SYS_CTX, nodeName, null, null);
        resDfn = resourceDefinitionFactory.create(
            SYS_CTX,
            resName,
            null,
            resPort,
            null,
            "secret",
            TransportType.IP,
            Arrays.asList(DeviceLayerKind.DRBD, DeviceLayerKind.STORAGE),
            null,
            createDefaultResourceGroup(SYS_CTX)
        );

        resUuid = randomUUID();
        objProt = objectProtectionFactory.getInstance(SYS_CTX, ObjectProtection.buildPath(nodeName, resName), true);

        initFlags = Resource.Flags.CLEAN.flagValue;
    }

    @Test
    public void testPersist() throws Exception
    {
        Resource res = TestFactory.createResource(
            resUuid,
            objProt,
            resDfn,
            node,
            initFlags,
            driver,
            propsContainerFactory,
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
        assertEquals(Resource.Flags.CLEAN.flagValue, resultSet.getLong(RESOURCE_FLAGS));
        assertFalse("Database persisted too many resources / resourceDefinitions", resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testPersistGetInstance() throws Exception
    {
        resourceFactory.create(
            SYS_CTX,
            resDfn,
            node,
            nodeId,
            new Resource.Flags[] {Resource.Flags.DELETE},
            Collections.emptyList()
        );

        commit();

        PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_RESOURCES);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue("Database did not persist resource / resourceDefinition", resultSet.next());
        // uuid is now != resUuid because getInstance create a new resData object
        assertEquals(nodeName.value, resultSet.getString(NODE_NAME));
        assertEquals(resName.value, resultSet.getString(RESOURCE_NAME));
        assertEquals(Resource.Flags.DELETE.flagValue, resultSet.getLong(RESOURCE_FLAGS));
        assertFalse("Database persisted too many resources / resourceDefinitions", resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testLoadGetInstance() throws Exception
    {
        Resource loadedRes = node.getResource(SYS_CTX, resDfn.getName());
        assertNull(loadedRes);

        Resource res = TestFactory.createResource(
            resUuid,
            objProt,
            resDfn,
            node,
            initFlags,
            driver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider,
            new TreeMap<>(),
            new TreeMap<>()
        );
        driver.create(res);
        node.addResource(SYS_CTX, res);
        resDfn.addResource(SYS_CTX, res);

        loadedRes = node.getResource(SYS_CTX, resDfn.getName());

        assertNotNull("Database did not persist resource / resourceDefinition", loadedRes);
        assertEquals(resUuid, loadedRes.getUuid());
        assertNotNull(loadedRes.getNode());
        assertEquals(nodeName, loadedRes.getNode().getName());
        assertNotNull(loadedRes.getDefinition());
        assertEquals(resName, loadedRes.getDefinition().getName());
        assertEquals(Resource.Flags.CLEAN.flagValue, loadedRes.getStateFlags().getFlagsBits(SYS_CTX));
    }

    @Test
    public void testLoadAll() throws Exception
    {
        Resource res = TestFactory.createResource(
            resUuid,
            objProt,
            resDfn,
            node,
            initFlags,
            driver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider,
            new TreeMap<>(),
            new TreeMap<>()
        );
        driver.create(res);

        nodesMap.put(nodeName, node);
        rscDfnMap.put(resName, resDfn);

        Map<Resource, Resource.InitMaps> resList = driver.loadAll(nodesMap, rscDfnMap);

        assertNotNull(resList);
        assertEquals(1, resList.size());

        Resource resData = resList.keySet().iterator().next();

        assertNotNull(resData);
        assertEquals(resUuid, resData.getUuid());
        assertNotNull(resData.getNode());
        assertEquals(nodeName, resData.getNode().getName());
        assertNotNull(resData.getDefinition());
        assertEquals(resName, resData.getDefinition().getName());
        assertEquals(Resource.Flags.CLEAN.flagValue, resData.getStateFlags().getFlagsBits(SYS_CTX));
    }

    @Test
    public void testCache() throws Exception
    {
        Resource storedInstance = resourceFactory.create(
            SYS_CTX,
            resDfn,
            node,
            nodeId,
            null,
            Collections.emptyList()
        );

        // no clearCaches

        assertEquals(storedInstance, node.getResource(SYS_CTX, resDfn.getName()));
    }

    @Test
    public void testDelete() throws Exception
    {
        Resource res = TestFactory.createResource(
            resUuid,
            objProt,
            resDfn,
            node,
            initFlags,
            driver,
            propsContainerFactory,
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
        Resource res = TestFactory.createResource(
            resUuid,
            objProt,
            resDfn,
            node,
            initFlags,
            driver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider,
            new TreeMap<>(),
            new TreeMap<>()
        );
        driver.create(res);
        commit();
        StateFlagsPersistence<Resource> stateFlagPersistence = driver.getStateFlagPersistence();
        stateFlagPersistence.persist(res, StateFlagsBits.getMask(Resource.Flags.DELETE));
        commit();

        PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_RESOURCES);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        assertEquals(Resource.Flags.DELETE.flagValue, resultSet.getLong(RESOURCE_FLAGS));

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

        Resource res = TestFactory.createResource(
            resUuid,
            objProt,
            resDfn,
            node,
            initFlags,
            driver,
            propsContainerFactory,
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
        Resource res = TestFactory.createResource(
            resUuid,
            objProt,
            resDfn,
            node,
            initFlags,
            driver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider,
            new TreeMap<>(),
            new TreeMap<>()
        );
        driver.create(res);
        node.addResource(SYS_CTX, res);
        resDfn.addResource(SYS_CTX, res);

        resourceFactory.create(SYS_CTX, resDfn, node, nodeId, null, Collections.emptyList());
    }
}
