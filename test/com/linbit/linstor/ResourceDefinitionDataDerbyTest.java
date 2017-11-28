package com.linbit.linstor;

import com.linbit.ImplementationError;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

import com.linbit.InvalidNameException;
import com.linbit.SatelliteTransactionMgr;
import com.linbit.TransactionMgr;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.MinorNumber;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.NodeId;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceData;
import com.linbit.linstor.ResourceDefinitionData;
import com.linbit.linstor.ResourceDefinitionDataDerbyDriver;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.TcpPortNumber;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.VolumeDefinitionData;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.Resource.RscFlags;
import com.linbit.linstor.ResourceDefinition.RscDfnFlags;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.security.DerbyBase;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.utils.UuidUtils;
import java.util.List;

public class ResourceDefinitionDataDerbyTest extends DerbyBase
{
    private static final String SELECT_ALL_RESOURCE_DEFINITIONS =
        " SELECT " + UUID + ", " + RESOURCE_NAME + ", " + RESOURCE_DSP_NAME + ", " +
                     TCP_PORT + ", " + SECRET + ", " + RESOURCE_FLAGS +
        " FROM " + TBL_RESOURCE_DEFINITIONS;

    private final ResourceName resName;
    private final TcpPortNumber port;
    private final NodeName nodeName;
    private final String secret;

    private TransactionMgr transMgr;
    private java.util.UUID resDfnUuid;
    private ObjectProtection resDfnObjProt;

    private NodeId node1Id;
    private Node node1;

    private ResourceDefinitionData resDfn;
    private ResourceDefinitionDataDerbyDriver driver;

    public ResourceDefinitionDataDerbyTest() throws InvalidNameException, ValueOutOfRangeException
    {
        resName = new ResourceName("TestResName");
        port = new TcpPortNumber(4242);
        nodeName = new NodeName("TestNodeName1");
        secret = "secret";
    }

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        assertEquals(TBL_RESOURCE_DEFINITIONS + " table's column count has changed. Update tests accordingly!", 6, TBL_COL_COUNT_RESOURCE_DEFINITIONS);

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
            port,
            RscDfnFlags.DELETE.flagValue,
            secret,
            transMgr
        );


        driver = (ResourceDefinitionDataDerbyDriver) LinStor.getResourceDefinitionDataDatabaseDriver();
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
        assertEquals(port.value, resultSet.getInt(TCP_PORT));
        assertEquals(secret, resultSet.getString(SECRET));
        assertEquals(RscDfnFlags.DELETE.flagValue, resultSet.getLong(RESOURCE_FLAGS));
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
            port,
            new RscDfnFlags[] { RscDfnFlags.DELETE },
            secret,
            transMgr,
            true,
            false
        );

        transMgr.commit();

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_RESOURCE_DEFINITIONS);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue("Database did not persist resource / resourceDefinition", resultSet.next());
        // uuid is now != resUuid because getInstance create a new resData object
        assertEquals(resName.value, resultSet.getString(RESOURCE_NAME));
        assertEquals(resName.displayValue, resultSet.getString(RESOURCE_DSP_NAME));
        assertEquals(port.value, resultSet.getInt(TCP_PORT));
        assertEquals(RscDfnFlags.DELETE.flagValue, resultSet.getLong(RESOURCE_FLAGS));
        assertEquals(secret, resultSet.getString(SECRET));
        assertFalse("Database persisted too many resources / resourceDefinitions", resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testLoad() throws Exception
    {
        driver.create(resDfn, transMgr);
        resDfnMap.put(resName, resDfn);
        ResourceData.getInstance(
            sysCtx,
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
        assertEquals(port, loadedResDfn.getPort(sysCtx));
        assertEquals(secret, loadedResDfn.getSecret(sysCtx));
        assertEquals(RscDfnFlags.DELETE.flagValue, loadedResDfn.getFlags().getFlagsBits(sysCtx));
    }

    @Test
    public void testLoadGetInstance() throws Exception
    {
        ResourceDefinitionData loadedResDfn = ResourceDefinitionData.getInstance(
            sysCtx,
            resName,
            port,
            new RscDfnFlags[] { RscDfnFlags.DELETE },
            secret,
            transMgr,
            false,
            false
        );

        assertNull(loadedResDfn);

        driver.create(resDfn, transMgr);

        resDfnMap.put(resDfn.getName(), resDfn);

        ResourceData.getInstance(
            sysCtx,
            resDfn,
            node1,
            node1Id,
            null,
            transMgr,
            true,
            false
        );

        loadedResDfn = ResourceDefinitionData.getInstance(
            sysCtx,
            resName,
            null,
            null,
            "secret",
            transMgr,
            false,
            false
        );

        assertNotNull("Database did not persist resource / resourceDefinition", loadedResDfn);
        assertEquals(resDfnUuid, loadedResDfn.getUuid());
        assertEquals(resName, loadedResDfn.getName());
        assertEquals(port, loadedResDfn.getPort(sysCtx));
        assertEquals(secret, loadedResDfn.getSecret(sysCtx));
        assertEquals(RscDfnFlags.DELETE.flagValue, loadedResDfn.getFlags().getFlagsBits(sysCtx));
    }

    @Test
    public void testCache() throws Exception
    {
        ResourceDefinitionData storedInstance = ResourceDefinitionData.getInstance(
            sysCtx,
            resName,
            port,
            null,
            secret,
            transMgr,
            true,
            false
        );
        super.resDfnMap.put(resName, storedInstance);
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

        Props props = resDfn.getProps(sysCtx);
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
        resDfnMap.put(resDfn.getName(), resDfn);
        NodeName nodeName = new NodeName("TestNodeName");
        Node node = NodeData.getInstance(sysCtx, nodeName, null, null, transMgr, true, false);
        nodesMap.put(node.getName(), node);

        NodeId nodeId = new NodeId(13);
        ResourceData res = ResourceData.getInstance(
            sysCtx,
            resDfn,
            node,
            nodeId,
            new RscFlags[] { RscFlags.CLEAN },
            transMgr,
            true,
            false
        );

        ResourceDefinitionData loadedResDfn = driver.load(resName, true, transMgr);
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
        VolumeDefinitionData volDfn = VolumeDefinitionData.getInstance(
            sysCtx,
            resDfn,
            volNr,
            minor,
            volSize,
            null,
            transMgr,
            true,
            false
        );

        ResourceDefinitionData loadedResDfn = driver.load(resName, true, transMgr);
        VolumeDefinition loadedVolDfn = loadedResDfn.getVolumeDfn(sysCtx, volNr);

        assertNotNull(loadedVolDfn);
        assertEquals(volDfn.getUuid(), loadedVolDfn.getUuid());
        assertEquals(volDfn.getFlags().getFlagsBits(sysCtx), loadedVolDfn.getFlags().getFlagsBits(sysCtx));
        assertEquals(minor, loadedVolDfn.getMinorNr(sysCtx));
        assertEquals(volNr, loadedVolDfn.getVolumeNumber());
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
    public void testUpdatePort() throws Exception
    {
        driver.create(resDfn, transMgr);
        resDfn.initialized();
        resDfn.setConnection(transMgr);

        TcpPortNumber otherPort = new TcpPortNumber(9001);
        resDfn.setPort(sysCtx, otherPort);
        transMgr.commit();

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_RESOURCE_DEFINITIONS);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        assertEquals(otherPort.value, resultSet.getInt(TCP_PORT));

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
        satelliteMode();
        ResourceDefinitionData instance = ResourceDefinitionData.getInstance(
            sysCtx,
            resName,
            port,
            new RscDfnFlags[] { RscDfnFlags.DELETE },
            "secret",
            null,
            true,
            false
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
    public void testGetInstanceNoCreate() throws Exception
    {
        satelliteMode();
        ResourceDefinitionData instance = ResourceDefinitionData.getInstance(
            sysCtx,
            resName,
            port,
            new RscDfnFlags[] { RscDfnFlags.DELETE },
            "secret",
            null,
            false,
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
        for(ResourceDefinitionData rdd : listResourceDefData)
        {
            if(rdd.getName().equals(spName))
                return rdd;
        }
        return null;
    }

    @Test
    public void testLoadAll() throws Exception
    {
        driver.create(resDfn, transMgr);
        ResourceName resName2 = new ResourceName("ResName2");
        ResourceDefinitionData.getInstance(
            sysCtx,
            resName2,
            port,
            null,
            "secret",
            transMgr,
            true,
            false
        );

        clearCaches();

        List<ResourceDefinitionData> resourceDefDataList = driver.loadAll(transMgr);

        ResourceDefinitionData res1 = findResourceDefinitionDatabyName(resourceDefDataList, resName);
        ResourceDefinitionData res2 = findResourceDefinitionDatabyName(resourceDefDataList, resName2);
        assertNotNull(res1);
        assertNotNull(res2);
        assertNotEquals(res1, res2);
    }

    @Test
    public void testDirtyParent() throws Exception
    {
        satelliteMode();
        SatelliteTransactionMgr transMgr = new SatelliteTransactionMgr();
        ResourceDefinitionData rscDfn = ResourceDefinitionData.getInstanceSatellite(
            sysCtx,
            resDfnUuid,
            resName,
            port,
            null,
            "notTellingYou",
            transMgr
        );
        rscDfn.getProps(sysCtx).setProp("test", "make this rscDfn dirty");

        VolumeDefinitionData vlmDfn = VolumeDefinitionData.getInstanceSatellite(
            sysCtx,
            java.util.UUID.randomUUID(),
            rscDfn,
            new VolumeNumber(0),
            1000,
            new MinorNumber(10),
            null,
            transMgr
        );
        vlmDfn.setConnection(transMgr);

    }

    @Test (expected = ImplementationError.class)
    /**
     * Check that an active transaction on an object can't be replaced
     */
    public void testReplaceActiveTransaction() throws Exception
    {
        satelliteMode();
        SatelliteTransactionMgr transMgr = new SatelliteTransactionMgr();
        ResourceDefinitionData rscDfn = ResourceDefinitionData.getInstanceSatellite(
            sysCtx,
            resDfnUuid,
            resName,
            port,
            null,
            "notTellingYou",
            transMgr
        );
        SatelliteTransactionMgr transMgrOther = new SatelliteTransactionMgr();
        rscDfn.setConnection(transMgrOther); // throws ImplementationError
        rscDfn.getProps(sysCtx).setProp("test", "make this rscDfn dirty");
    }

    @Test
    /**
     * This test checks that a new transaction can be set after a commit.
     */
    public void testNewTransaction() throws Exception
    {
        satelliteMode();
        SatelliteTransactionMgr transMgr = new SatelliteTransactionMgr();
        ResourceDefinitionData rscDfn = ResourceDefinitionData.getInstanceSatellite(
            sysCtx,
            resDfnUuid,
            resName,
            port,
            null,
            "notTellingYou",
            transMgr
        );
        transMgr.commit();

        assertEquals(0, transMgr.sizeObjects());
        assertFalse(rscDfn.hasTransMgr());

        SatelliteTransactionMgr transMgrOther = new SatelliteTransactionMgr();
        rscDfn.setConnection(transMgrOther);
        rscDfn.getProps(sysCtx).setProp("test", "make this rscDfn dirty");
        assertTrue(rscDfn.hasTransMgr());
        assertTrue(rscDfn.isDirty());
    }

    @Test (expected = ImplementationError.class)
    /**
     * This test should fail because the resourcedef properties are changed without an active transaction.
     */
    public void testDirtyObject() throws Exception
    {
        satelliteMode();
        SatelliteTransactionMgr transMgr = new SatelliteTransactionMgr();
        ResourceDefinitionData rscDfn = ResourceDefinitionData.getInstanceSatellite(
            sysCtx,
            resDfnUuid,
            resName,
            port,
            null,
            "notTellingYou",
            transMgr
        );
        transMgr.commit();

        rscDfn.getProps(sysCtx).setProp("test", "make this rscDfn dirty");
        SatelliteTransactionMgr transMgrOther = new SatelliteTransactionMgr();
        rscDfn.setConnection(transMgrOther); // throws ImplementationError
    }


    @Test (expected = LinStorDataAlreadyExistsException.class)
    public void testAlreadyExists() throws Exception
    {
        driver.create(resDfn, transMgr);

        ResourceDefinitionData.getInstance(sysCtx, resName, port, null, "secret", transMgr, false, true);
    }
}
