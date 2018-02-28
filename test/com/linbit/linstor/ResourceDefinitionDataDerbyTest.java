package com.linbit.linstor;

import com.google.inject.Inject;
import com.linbit.InvalidNameException;
import com.linbit.TransactionMgr;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.Resource.RscFlags;
import com.linbit.linstor.ResourceDefinition.RscDfnFlags;
import com.linbit.linstor.ResourceDefinition.TransportType;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.security.DerbyBase;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.utils.UuidUtils;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ResourceDefinitionDataDerbyTest extends DerbyBase
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

    private TransactionMgr transMgr;
    private java.util.UUID resDfnUuid;
    private ObjectProtection resDfnObjProt;

    private NodeId node1Id;
    private Node node1;

    private ResourceDefinitionData resDfn;
    @Inject private ResourceDefinitionDataDerbyDriver driver;

    public ResourceDefinitionDataDerbyTest() throws InvalidNameException, ValueOutOfRangeException
    {
        resName = new ResourceName("TestResName");
        port = 4242;
        nodeName = new NodeName("TestNodeName1");
        secret = "secret";
        transportType = TransportType.IP;
    }

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        assertEquals(TBL_RESOURCE_DEFINITIONS + " table's column count has changed. Update tests accordingly!", 7, TBL_COL_COUNT_RESOURCE_DEFINITIONS);

        transMgr = new TransactionMgr(getConnection());

        resDfnUuid = randomUUID();

        resDfnObjProt = objectProtectionFactory.getInstance(
            SYS_CTX,
            ObjectProtection.buildPath(resName),
            true,
            transMgr
        );

        node1Id = new NodeId(1);
        node1 = nodeDataFactory.getInstance(
            SYS_CTX,
            nodeName,
            null,
            null,
            transMgr,
            true,
            false
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
            transMgr,
            driver,
            propsContainerFactory
        );
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
            new RscDfnFlags[] { RscDfnFlags.DELETE },
            secret,
            transportType,
            transMgr
        );

        transMgr.commit();

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_RESOURCE_DEFINITIONS);
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
    public void testLoad() throws Exception
    {
        driver.create(resDfn, transMgr);
        rscDfnMap.put(resName, resDfn);
        resourceDataFactory.getInstance(
            SYS_CTX,
            resDfn,
            node1,
            node1Id,
            null,
            transMgr,
            true,
            false
        );

        ResourceDefinitionData loadedResDfn = driver.load(resName, true, transMgr);

        assertNotNull("Database did not persist resource / resourceDefinition", loadedResDfn);
        assertEquals(resDfnUuid, loadedResDfn.getUuid());
        assertEquals(resName, loadedResDfn.getName());
        assertEquals(port, loadedResDfn.getPort(SYS_CTX).value);
        assertEquals(secret, loadedResDfn.getSecret(SYS_CTX));
        assertEquals(transportType, loadedResDfn.getTransportType(SYS_CTX));
        assertEquals(RscDfnFlags.DELETE.flagValue, loadedResDfn.getFlags().getFlagsBits(SYS_CTX));
    }

    @Test
    public void testLoadGetInstance() throws Exception
    {
        ResourceDefinitionData loadedResDfn = resourceDefinitionDataFactory.load(
            resName,
            transMgr
        );

        assertNull(loadedResDfn);

        driver.create(resDfn, transMgr);

        rscDfnMap.put(resDfn.getName(), resDfn);

        resourceDataFactory.getInstance(
            SYS_CTX,
            resDfn,
            node1,
            node1Id,
            null,
            transMgr,
            true,
            false
        );

        loadedResDfn = resourceDefinitionDataFactory.load(
            resName,
            transMgr
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
            transportType,
            transMgr
        );
        rscDfnMap.put(resName, storedInstance);
        // no clearCaches

        assertEquals(storedInstance, driver.load(resName, true, transMgr));
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

        Props props = resDfn.getProps(SYS_CTX);
        String testKey = "TestKey";
        String testValue = "TestValue";
        props.setProp(testKey, testValue);

        transMgr.commit();

        Map<String, String> testMap = new HashMap<>();
        testMap.put(testKey, testValue);
        testProps(transMgr, PropsContainer.buildPath(resName), testMap);
    }

    @Test
    public void testLoadResources() throws Exception
    {
        driver.create(resDfn, transMgr);
        rscDfnMap.put(resDfn.getName(), resDfn);
        NodeName nodeName = new NodeName("TestNodeName");
        Node node = nodeDataFactory.getInstance(SYS_CTX, nodeName, null, null, transMgr, true, false);
        nodesMap.put(node.getName(), node);

        NodeId nodeId = new NodeId(13);
        ResourceData res = resourceDataFactory.getInstance(
            SYS_CTX,
            resDfn,
            node,
            nodeId,
            new RscFlags[] { RscFlags.CLEAN },
            transMgr,
            true,
            false
        );

        ResourceDefinitionData loadedResDfn = driver.load(resName, true, transMgr);
        Resource loadedRes = loadedResDfn.getResource(SYS_CTX, nodeName);

        assertNotNull(loadedRes);
        assertEquals(nodeName, loadedRes.getAssignedNode().getName());
        assertEquals(loadedResDfn, loadedRes.getDefinition());
        assertEquals(nodeId, loadedRes.getNodeId());
        assertNotNull(loadedRes.getObjProt());
        assertNotNull(loadedRes.getProps(SYS_CTX));
        assertEquals(RscFlags.CLEAN.flagValue, loadedRes.getStateFlags().getFlagsBits(SYS_CTX));
        assertEquals(res.getUuid(), loadedRes.getUuid());
    }

    @Test
    public void testLoadVolumeDefinitions() throws Exception
    {
        driver.create(resDfn, transMgr);

        VolumeNumber volNr = new VolumeNumber(13);
        int minor = 42;
        long volSize = 5_000;
        VolumeDefinitionData volDfn = volumeDefinitionDataFactory.create(
            SYS_CTX,
            resDfn,
            volNr,
            minor,
            volSize,
            null,
            transMgr
        );

        ResourceDefinitionData loadedResDfn = driver.load(resName, true, transMgr);
        VolumeDefinition loadedVolDfn = loadedResDfn.getVolumeDfn(SYS_CTX, volNr);

        assertNotNull(loadedVolDfn);
        assertEquals(volDfn.getUuid(), loadedVolDfn.getUuid());
        assertEquals(volDfn.getFlags().getFlagsBits(SYS_CTX), loadedVolDfn.getFlags().getFlagsBits(SYS_CTX));
        assertEquals(minor, loadedVolDfn.getMinorNr(SYS_CTX).value);
        assertEquals(volNr, loadedVolDfn.getVolumeNumber());
        assertEquals(volSize, loadedVolDfn.getVolumeSize(SYS_CTX));
        assertEquals(loadedResDfn, loadedVolDfn.getResourceDefinition());
    }

    @Test
    public void testStateFlagPersistence() throws Exception
    {
        driver.create(resDfn, transMgr);
        resDfn.initialized();
        resDfn.setConnection(transMgr);

        resDfn.getFlags().disableAllFlags(SYS_CTX);

        transMgr.commit();

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_RESOURCE_DEFINITIONS);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        assertEquals(0, resultSet.getLong(RESOURCE_FLAGS));

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testUpdatePort() throws Exception
    {
        driver.create(resDfn, transMgr);
        resDfn.initialized();
        resDfn.setConnection(transMgr);

        TcpPortNumber otherPort = new TcpPortNumber(9001);
        resDfn.setPort(SYS_CTX, otherPort);
        transMgr.commit();

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_RESOURCE_DEFINITIONS);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        assertEquals(otherPort.value, resultSet.getInt(TCP_PORT));

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testUpdateTransportType() throws Exception
    {
        driver.create(resDfn, transMgr);
        resDfn.initialized();
        resDfn.setConnection(transMgr);

        TransportType newTransportType = TransportType.RDMA;
        resDfn.setTransportType(SYS_CTX, newTransportType);
        transMgr.commit();

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_RESOURCE_DEFINITIONS);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        assertEquals(newTransportType.name(), resultSet.getString(TRANSPORT_TYPE));

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
    public void testHalfValidName() throws Exception
    {
        driver.create(resDfn, transMgr);

        ResourceName halfValidResName = new ResourceName(resDfn.getName().value);

        ResourceDefinitionData loadedResDfn = driver.load(halfValidResName, true, transMgr);

        assertNotNull(loadedResDfn);
        assertEquals(resDfn.getName(), loadedResDfn.getName());
        assertEquals(resDfn.getUuid(), loadedResDfn.getUuid());
    }

    private ResourceDefinitionData findResourceDefinitionDatabyName(
            List<ResourceDefinitionData> listResourceDefData,
            ResourceName spName)
    {
        ResourceDefinitionData rscDfnData = null;
        for (ResourceDefinitionData rdd : listResourceDefData)
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
        driver.create(resDfn, transMgr);
        ResourceName resName2 = new ResourceName("ResName2");
        resourceDefinitionDataFactory.create(
            SYS_CTX,
            resName2,
            port,
            null,
            "secret",
            transportType,
            transMgr
        );

        clearCaches();

        List<ResourceDefinitionData> resourceDefDataList = driver.loadAll(transMgr);

        ResourceDefinitionData res1 = findResourceDefinitionDatabyName(resourceDefDataList, resName);
        ResourceDefinitionData res2 = findResourceDefinitionDatabyName(resourceDefDataList, resName2);
        assertNotNull(res1);
        assertNotNull(res2);
        assertNotEquals(res1, res2);
    }

    @Test (expected = LinStorDataAlreadyExistsException.class)
    public void testAlreadyExists() throws Exception
    {
        driver.create(resDfn, transMgr);

        resourceDefinitionDataFactory.create(
            SYS_CTX, resName, port, null, "secret", transportType, transMgr
        );
    }
}
