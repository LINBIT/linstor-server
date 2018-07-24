package com.linbit.linstor;

import javax.inject.Inject;
import com.linbit.InvalidNameException;
import com.linbit.linstor.ResourceDefinition.InitMaps;
import com.linbit.linstor.ResourceDefinition.RscDfnFlags;
import com.linbit.linstor.ResourceDefinition.TransportType;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.security.GenericDbBase;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionDatabaseDriver;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ResourceDefinitionDataGenericDbDriverTest extends GenericDbBase
{
    private static final String SELECT_ALL_RESOURCE_DEFINITIONS =
        " SELECT " + UUID + ", " + RESOURCE_NAME + ", " + RESOURCE_DSP_NAME + ", " +
                     TCP_PORT + ", " + SECRET + ", " + RESOURCE_FLAGS + ", " + TRANSPORT_TYPE +
        " FROM " + TBL_RESOURCE_DEFINITIONS;

    private final ResourceName resName;
    private final int port;
    private final NodeName nodeName;
    private final String secret;
    private final TransportType transportType;

    private java.util.UUID resDfnUuid;
    private ObjectProtection resDfnObjProt;

    private NodeId node1Id;
    private Node node1;

    private ResourceDefinitionData resDfn;
    @Inject private ResourceDefinitionDataGenericDbDriver driver;
    @Inject private ObjectProtectionDatabaseDriver objProtDriver;

    @SuppressWarnings("checkstyle:magicnumber")
    public ResourceDefinitionDataGenericDbDriverTest() throws InvalidNameException
    {
        resName = new ResourceName("TestResName");
        port = 4242;
        nodeName = new NodeName("TestNodeName1");
        secret = "secret";
        transportType = TransportType.IP;
    }

    @SuppressWarnings("checkstyle:magicnumber")
    @Before
    public void setUp() throws Exception
    {
        super.setUpAndEnterScope();
        assertEquals(
            TBL_RESOURCE_DEFINITIONS + " table's column count has changed. Update tests accordingly!",
            7,
            TBL_COL_COUNT_RESOURCE_DEFINITIONS
        );

        resDfnUuid = randomUUID();

        resDfnObjProt = createTestObjectProtection(
            SYS_CTX,
            ObjectProtection.buildPath(resName)
        );

        node1Id = new NodeId(1);
        node1 = nodeDataFactory.create(
            SYS_CTX,
            nodeName,
            null,
            null
        );
        nodesMap.put(node1.getName(), node1);
        resDfn = new ResourceDefinitionData(
            resDfnUuid,
            resDfnObjProt,
            resName,
            new TcpPortNumber(port),
            tcpPortPoolMock,
            RscDfnFlags.DELETE.flagValue,
            secret,
            transportType,
            driver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider,
            new TreeMap<>(),
            new TreeMap<>(),
            new TreeMap<>()
        );
    }

    @Test
    public void testPersist() throws Exception
    {
        driver.create(resDfn);
        commit();

        PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_RESOURCE_DEFINITIONS);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue("Database did not persist resourceDefinition", resultSet.next());
        assertEquals(resDfnUuid, java.util.UUID.fromString(resultSet.getString(UUID)));
        assertEquals(resName.value, resultSet.getString(RESOURCE_NAME));
        assertEquals(resName.displayValue, resultSet.getString(RESOURCE_DSP_NAME));
        assertEquals(port, resultSet.getInt(TCP_PORT));
        assertEquals(secret, resultSet.getString(SECRET));
        assertEquals(RscDfnFlags.DELETE.flagValue, resultSet.getLong(RESOURCE_FLAGS));
        assertEquals(ResourceDefinition.TransportType.IP.name(), resultSet.getString(TRANSPORT_TYPE));
        assertFalse("Database persisted too many resourceDefinitions", resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testPersistGetInstance() throws Exception
    {
        resourceDefinitionDataFactory.create(
            SYS_CTX,
            resName,
            port,
            new RscDfnFlags[] {RscDfnFlags.DELETE},
            secret,
            transportType
        );

        commit();

        PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_RESOURCE_DEFINITIONS);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue("Database did not persist resource / resourceDefinition", resultSet.next());
        // uuid is now != resUuid because getInstance create a new resData object
        assertEquals(resName.value, resultSet.getString(RESOURCE_NAME));
        assertEquals(resName.displayValue, resultSet.getString(RESOURCE_DSP_NAME));
        assertEquals(port, resultSet.getInt(TCP_PORT));
        assertEquals(RscDfnFlags.DELETE.flagValue, resultSet.getLong(RESOURCE_FLAGS));
        assertEquals(secret, resultSet.getString(SECRET));
        assertEquals(transportType.name(), resultSet.getString(TRANSPORT_TYPE));
        assertFalse("Database persisted too many resources / resourceDefinitions", resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testLoadGetInstance() throws Exception
    {
        ResourceDefinitionData loadedResDfn = resourceDefinitionDataFactory.getInstance(
            SYS_CTX,
            resName
        );

        assertNull(loadedResDfn);

        driver.create(resDfn);

        rscDfnMap.put(resDfn.getName(), resDfn);

        resourceDataFactory.create(
            SYS_CTX,
            resDfn,
            node1,
            node1Id,
            null
        );

        loadedResDfn = resourceDefinitionDataFactory.getInstance(
            SYS_CTX,
            resName
        );

        assertNotNull("Database did not persist resource / resourceDefinition", loadedResDfn);
        assertEquals(resDfnUuid, loadedResDfn.getUuid());
        assertEquals(resName, loadedResDfn.getName());
        assertEquals(port, loadedResDfn.getPort(SYS_CTX).value);
        assertEquals(secret, loadedResDfn.getSecret(SYS_CTX));
        assertEquals(transportType, loadedResDfn.getTransportType(SYS_CTX));
        assertEquals(RscDfnFlags.DELETE.flagValue, loadedResDfn.getFlags().getFlagsBits(SYS_CTX));
    }

    @Test
    public void testCache() throws Exception
    {
        ResourceDefinitionData storedInstance = resourceDefinitionDataFactory.create(
            SYS_CTX,
            resName,
            port,
            null,
            secret,
            transportType
        );
        rscDfnMap.put(resName, storedInstance);
        // no clearCaches

        assertEquals(storedInstance, resourceDefinitionDataFactory.getInstance(
            SYS_CTX,
            resName
        ));
    }

    @Test
    public void testDelete() throws Exception
    {
        driver.create(resDfn);
        commit();

        PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_RESOURCE_DEFINITIONS);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue("Database did not persist resourceDefinition", resultSet.next());
        resultSet.close();

        driver.delete(resDfn);
        commit();

        resultSet = stmt.executeQuery();
        assertFalse("Database did not delete resourceDefinition", resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testPersistProps() throws Exception
    {
        driver.create(resDfn);

        Props props = resDfn.getProps(SYS_CTX);
        String testKey = "TestKey";
        String testValue = "TestValue";
        props.setProp(testKey, testValue);

        commit();

        Map<String, String> testMap = new HashMap<>();
        testMap.put(testKey, testValue);
        testProps(PropsContainer.buildPath(resName), testMap);
    }

    @Test
    public void testStateFlagPersistence() throws Exception
    {
        driver.create(resDfn);

        resDfn.getFlags().disableAllFlags(SYS_CTX);

        commit();

        PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_RESOURCE_DEFINITIONS);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        assertEquals(0, resultSet.getLong(RESOURCE_FLAGS));

        resultSet.close();
        stmt.close();
    }

    @SuppressWarnings("checkstyle:magicnumber")
    @Test
    public void testUpdatePort() throws Exception
    {
        driver.create(resDfn);

        TcpPortNumber otherPort = new TcpPortNumber(9001);
        resDfn.setPort(SYS_CTX, otherPort);
        commit();

        PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_RESOURCE_DEFINITIONS);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        assertEquals(otherPort.value, resultSet.getInt(TCP_PORT));

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testUpdateTransportType() throws Exception
    {
        driver.create(resDfn);

        TransportType newTransportType = TransportType.RDMA;
        resDfn.setTransportType(SYS_CTX, newTransportType);
        commit();

        PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_RESOURCE_DEFINITIONS);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        assertEquals(newTransportType.name(), resultSet.getString(TRANSPORT_TYPE));

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testExists() throws Exception
    {
        assertFalse(driver.exists(resName));
        driver.create(resDfn);
        assertTrue(driver.exists(resName));
    }

    private ResourceDefinitionData findResourceDefinitionDatabyName(
        Map<ResourceDefinitionData, InitMaps> resourceDefDataMap,
        ResourceName spName)
    {
        ResourceDefinitionData rscDfnData = null;
        for (ResourceDefinitionData rdd : resourceDefDataMap.keySet())
        {
            if (rdd.getName().equals(spName))
            {
                rscDfnData = rdd;
                break;
            }
        }
        return rscDfnData;
    }

    @Test
    public void testLoadAll() throws Exception
    {
        driver.create(resDfn);
        ResourceName resName2 = new ResourceName("ResName2");
        resourceDefinitionDataFactory.create(
            SYS_CTX,
            resName2,
            port + 1, // prevent tcp-port-conflict
            null,
            "secret",
            transportType
        );
        objProtDriver.insertOp(resDfnObjProt);

        clearCaches();

        Map<ResourceDefinitionData, InitMaps> resourceDefDataList = driver.loadAll();

        ResourceDefinitionData res1 = findResourceDefinitionDatabyName(resourceDefDataList, resName);
        ResourceDefinitionData res2 = findResourceDefinitionDatabyName(resourceDefDataList, resName2);
        assertNotNull(res1);
        assertNotNull(res2);
        assertNotEquals(res1, res2);
    }

    @Test (expected = LinStorDataAlreadyExistsException.class)
    public void testAlreadyExists() throws Exception
    {
        driver.create(resDfn);
        objProtDriver.insertOp(resDfnObjProt);
        rscDfnMap.put(resName, resDfn);

        resourceDefinitionDataFactory.create(
            SYS_CTX, resName, port, null, "secret", transportType
        );
    }

    @Test
    public void testAutoAllocateTcpPort() throws Exception
    {
        final int testTcpPort = 9876;

        Mockito.when(tcpPortPoolMock.autoAllocate()).thenReturn(testTcpPort);

        ResourceDefinitionData newRscDfn = resourceDefinitionDataFactory.create(
            SYS_CTX,
            resName,
            null, // auto allocate
            null,
            "secret",
            transportType
        );

        assertThat(newRscDfn.getPort(SYS_CTX).value).isEqualTo(testTcpPort);
    }

    @Test
    public void testDeleteDeallocateTcpPort() throws Exception
    {
        driver.create(resDfn);
        resDfn.delete(SYS_CTX);

        Mockito.verify(tcpPortPoolMock).deallocate(port);
    }

    @Test
    public void testModifyTcpPort() throws Exception
    {
        final int newTcpPort = 9876;

        driver.create(resDfn);
        resDfn.setPort(SYS_CTX, new TcpPortNumber(newTcpPort));

        Mockito.verify(tcpPortPoolMock).deallocate(port);
        Mockito.verify(tcpPortPoolMock).allocate(newTcpPort);
    }
}
