package com.linbit.drbdmanage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

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
    private Connection con;
    private TransactionMgr transMgr;
    private java.util.UUID resDfnUuid;
    private ObjectProtection resDfnObjProt;
    
    private ResourceDefinitionData resDfn;
    private ResourceDefinitionDataDerbyDriver driver;

    public ResourceDefinitionDataDerbyTest() throws InvalidNameException
    {
        resName = new ResourceName("TestResName");
    }

    @Before
    public void startUp() throws Exception
    {
        assertEquals(TBL_RESOURCE_DEFINITIONS + " table's column count has changed. Update tests accordingly!", 4, TBL_COL_COUNT_RESOURCE_DEFINITIONS);

        con = getConnection();
        transMgr = new TransactionMgr(con);

        resDfnUuid = randomUUID();
        
        resDfnObjProt = ObjectProtection.getInstance(
            sysCtx, 
            transMgr,
            ObjectProtection.buildPath(resName), 
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
        
        driver = (ResourceDefinitionDataDerbyDriver) DrbdManage.getResourceDefinitionDataDatabaseDriver(resName);
    }
    
    @Test
    public void testPersist() throws Exception
    {
        driver.create(con, resDfn);
        
        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_RESOURCE_DEFINITIONS);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue("Database did not persist resourceDefinition", resultSet.next());
        assertEquals(resDfnUuid, UuidUtils.asUUID(resultSet.getBytes(UUID)));
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

        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_RESOURCE_DEFINITIONS);
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
        driver.create(con, resDfn);

        DriverUtils.clearCaches();
        
        ResourceDefinitionData loadedResDfn = driver.load(con, null, transMgr);

        assertNotNull("Database did not persist resource / resourceDefinition", loadedResDfn);
        assertEquals(resDfnUuid, loadedResDfn.getUuid());
        assertEquals(resName, loadedResDfn.getName());
        assertEquals(RscDfnFlags.REMOVE.flagValue, loadedResDfn.getFlags().getFlagsBits(sysCtx));
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
            
        driver.create(con, resDfn);
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
	}

    @Test
	public void testDelete() throws Exception
	{
        driver.create(con, resDfn);
 
        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_RESOURCE_DEFINITIONS);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue("Database did not persist resourceDefinition", resultSet.next());
        resultSet.close();
        
        driver.delete(con);
        
        resultSet = stmt.executeQuery();
        assertFalse("Database did not delete resourceDefinition", resultSet.next());
        
        resultSet.close();
        stmt.close();
	}

    @Test
    public void testLoadConnectionDefinition() throws Exception
    {
        // TODO: implement test
    }

    @Test
    public void testPersistProps() throws Exception
    {
        resDfn.initialized();
        resDfn.setConnection(transMgr);
        driver.create(con, resDfn);
        
        Props props = resDfn.getProps(sysCtx);
        String testKey = "TestKey";
        String testValue = "TestValue";
        props.setProp(testKey, testValue);
        
        transMgr.commit();
        
        PreparedStatement stmt = selectProps(con, PropsContainer.buildPath(resName));
        ResultSet resultSet = stmt.executeQuery();
        
        int count = 0;
        while (resultSet.next())
        {
            String key = resultSet.getString(PROP_KEY);
            if (key.equals(testKey)) 
            {
                assertEquals(testValue, resultSet.getString(PROP_VALUE));
            }
            else
            {
                assertEquals(SerialGenerator.KEY_SERIAL, key);
            }
            ++count;
        }
        assertEquals(2, count);
        
        resultSet.close();
        stmt.close();
    }

    @Test
    public void testLoadProps() throws Exception
    {
        driver.create(con, resDfn);
        String testKey = "TestKey";
        String testValue = "TestValue";
        insertProp(con, PropsContainer.buildPath(resName), testKey, testValue);
        
        DriverUtils.clearCaches();
        
        ResourceDefinitionData loadedResDfn = driver.load(con, null, transMgr);
        
        Props props = loadedResDfn.getProps(sysCtx);
        
        assertNotNull(props);
        assertEquals(testValue, props.getProp(testKey));
        assertNotNull(props.getProp(SerialGenerator.KEY_SERIAL));
        assertEquals(2, props.size());
    }
    
    @Test
    public void testLoadResources() throws Exception
    {
        driver.create(con, resDfn);
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
        
        ResourceDefinitionData loadedResDfn = driver.load(con, null, transMgr);
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
        driver.create(con, resDfn);
        
        VolumeNumber volNr = new VolumeNumber(13);
        MinorNumber minor = new MinorNumber(42);
        long volSize = 5_000;
        VolumeDefinitionData volDfn = VolumeDefinitionData.getInstance(sysCtx, resDfn, volNr, minor, volSize, null, null, transMgr, true);
        
        DriverUtils.clearCaches();
        
        ResourceDefinitionData loadedResDfn = driver.load(con, null, transMgr);
        VolumeDefinition loadedVolDfn = loadedResDfn.getVolumeDfn(sysCtx, volNr);
        
        assertNotNull(loadedVolDfn);
        assertEquals(volDfn.getUuid(), loadedVolDfn.getUuid());
        assertEquals(volDfn.getFlags().getFlagsBits(sysCtx), loadedVolDfn.getFlags().getFlagsBits(sysCtx));
        assertEquals(minor, loadedVolDfn.getMinorNr(sysCtx));
        assertEquals(volNr, loadedVolDfn.getVolumeNumber(sysCtx));
        assertEquals(volSize, loadedVolDfn.getVolumeSize(sysCtx));
        assertEquals(loadedResDfn, loadedVolDfn.getResourceDfn());
    }
    
    @Test
	public void testStateFlagPersistence() throws Exception
	{
		driver.create(con, resDfn);

		resDfn.getFlags().disableAllFlags(sysCtx);
		
		PreparedStatement stmt = con.prepareStatement(SELECT_ALL_RESOURCE_DEFINITIONS);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        assertEquals(RscDfnFlags.REMOVE.flagValue, resultSet.getLong(RESOURCE_FLAGS));

        resultSet.close();
        stmt.close();
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
        
        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_RESOURCE_DEFINITIONS);
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
        
        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_RESOURCE_DEFINITIONS);
        ResultSet resultSet = stmt.executeQuery();

        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
	}
}
