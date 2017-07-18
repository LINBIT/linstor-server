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
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.linbit.InvalidNameException;
import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.dbdrivers.interfaces.ConnectionDefinitionDataDatabaseDriver;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.DerbyBase;
import com.linbit.drbdmanage.security.ObjectProtection;
import com.linbit.utils.UuidUtils;

public class ConnectionDefinitionDataDerbyTest extends DerbyBase
{
    private static final String SELECT_ALL_CON_DFNS =
        " SELECT " + UUID + ", " + RESOURCE_NAME + ", " +
                     NODE_NAME_SRC + ", " + NODE_NAME_DST + ", " + CON_NR +
        " FROM " + TBL_CONNECTION_DEFINITIONS;

    private final ResourceName resName;
    private final NodeName sourceName;
    private final NodeName targetName;
    private final int conNr;

    private Connection con;
    private TransactionMgr transMgr;

    private java.util.UUID uuid;
    private ObjectProtection objProt;
    private ResourceDefinitionData resDfn;
    private NodeData nodeSrc;
    private NodeData nodeDst;

    private ConnectionDefinitionData conDfn;

    private ConnectionDefinitionDataDatabaseDriver driver;
    private SingleColumnDatabaseDriver<Integer> conNrDriver;


    public ConnectionDefinitionDataDerbyTest() throws InvalidNameException
    {
        resName = new ResourceName("testResourceName");
        sourceName = new NodeName("testNodeSource");
        targetName = new NodeName("testNodeTarget");
        conNr = 13;
    }

    @Before
    public void startUp() throws Exception
    {
        con = getConnection();
        transMgr = new TransactionMgr(con);

        uuid = randomUUID();
        objProt = ObjectProtection.getInstance(
            sysCtx,
            transMgr,
            ObjectProtection.buildPath(
                resName,
                sourceName,
                targetName
            ),
            true
        );

        resDfn = ResourceDefinitionData.getInstance(sysCtx, resName, null, null, transMgr, true);
        nodeSrc = NodeData.getInstance(sysCtx, sourceName, null, null, null, transMgr, true);
        nodeDst = NodeData.getInstance(sysCtx, targetName, null, null, null, transMgr, true);

        conDfn = new ConnectionDefinitionData(uuid, objProt, resDfn, nodeSrc, nodeDst, conNr);
        driver = DrbdManage.getConnectionDefinitionDatabaseDriver(resName, sourceName, targetName);
        conNrDriver = driver.getConnectionNumberDriver();
    }

    @Test
    public void testPersist() throws Exception
    {
        driver.create(con, conDfn);

        checkDbPersist(true);
    }

    @Test
    public void testPersistGetInstance() throws Exception
    {
        ConnectionDefinitionData.getInstance(sysCtx, resDfn, nodeSrc, nodeDst, conNr, null, transMgr, true);

        checkDbPersist(false);
    }

    private void checkDbPersist(boolean checkUuid) throws SQLException
    {
        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_CON_DFNS);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        if (checkUuid)
        {
            assertEquals(uuid, UuidUtils.asUuid(resultSet.getBytes(UUID)));
        }
        assertEquals(resName.value, resultSet.getString(RESOURCE_NAME));
        assertEquals(sourceName.value, resultSet.getString(NODE_NAME_SRC));
        assertEquals(targetName.value, resultSet.getString(NODE_NAME_DST));
        assertEquals(conNr, resultSet.getInt(CON_NR));

        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testLoad() throws Exception
    {
        driver.create(con, conDfn);
        DriverUtils.clearCaches();

        ConnectionDefinitionData loadedConDfn = driver.load(con, null, transMgr);

        checkLoadedConDfn(loadedConDfn, true);
    }


    @Test
    public void testLoadStatic() throws Exception
    {
        driver.create(con, conDfn);
        DriverUtils.clearCaches();

        Map<NodeName, Map<Integer, ConnectionDefinition>> conMap =
            ConnectionDefinitionDataDerbyDriver.loadAllConnectionsByResourceDefinition(
                con,
                resName,
                null,
                transMgr,
                sysCtx
        );

        assertNotNull(conMap);
        Map<Integer, ConnectionDefinition> nodeConMap = conMap.get(sourceName);
        assertNotNull(nodeConMap);
        assertEquals(nodeConMap, conMap.get(targetName));

        ConnectionDefinition loadedConDfn = nodeConMap.get(conNr);
        checkLoadedConDfn(loadedConDfn, true);

        assertEquals(1, nodeConMap.size());

        assertEquals(2, conMap.size());
    }

    @Test
    public void testLoadGetInstance() throws Exception
    {
        driver.create(con, conDfn);
        DriverUtils.clearCaches();

        ConnectionDefinitionData loadedConDfn = ConnectionDefinitionData.getInstance(
            sysCtx,
            resDfn,
            nodeSrc,
            nodeDst,
            conNr,
            null,
            transMgr,
            false
        );

        checkLoadedConDfn(loadedConDfn, true);
    }

    private void checkLoadedConDfn(ConnectionDefinition loadedConDfn, boolean checkUuid) throws AccessDeniedException
    {
        assertNotNull(loadedConDfn);
        if (checkUuid)
        {
            assertEquals(uuid, loadedConDfn.getUuid());
        }
        assertEquals(conNr, loadedConDfn.getConnectionNumber(sysCtx));
        assertEquals(resName, loadedConDfn.getResourceDefinition(sysCtx).getName());
        assertEquals(sourceName, loadedConDfn.getSourceNode(sysCtx).getName());
        assertEquals(targetName, loadedConDfn.getTargetNode(sysCtx).getName());
    }

    @Test
    public void testCache() throws Exception
    {
        driver.create(con, conDfn);

        // no clear-cache

        assertEquals(conDfn, driver.load(con, null, transMgr));
    }

    @Test
    public void testDelete() throws Exception
    {
        driver.create(con, conDfn);
        DriverUtils.clearCaches();

        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_CON_DFNS);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        assertFalse(resultSet.next());
        resultSet.close();

        driver.delete(con);

        resultSet = stmt.executeQuery();

        assertFalse(resultSet.next());
        resultSet.close();

        stmt.close();
    }

    @Test
    public void testConNrDriverUpdate() throws Exception
    {
        driver.create(con, conDfn);

        int newConNr = 42;
        conNrDriver.update(con, newConNr);

        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_CON_DFNS);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());

        assertEquals(newConNr, resultSet.getInt(CON_NR));

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testConNrInstanceUpdate() throws Exception
    {
        driver.create(con, conDfn);

        conDfn.initialized();
        conDfn.setConnection(transMgr);
        conDfn.setConnectionNr(sysCtx, 42);

        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_CON_DFNS);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());

        assertEquals(conNr, resultSet.getInt(CON_NR));

        resultSet.close();
        stmt.close();
    }

    @Test
    public void testSatelliteCreate() throws Exception
    {
        ConnectionDefinitionData satelliteConDfn = ConnectionDefinitionData.getInstance(
            sysCtx,
            resDfn,
            nodeSrc,
            nodeDst,
            conNr,
            null,
            null,
            true
        );

        checkLoadedConDfn(satelliteConDfn, false);

        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_CON_DFNS);
        ResultSet resultSet = stmt.executeQuery();

        assertFalse(resultSet.next());
        resultSet.close();
        stmt.close();
    }

    @Test
    public void testSatelliteNoCreate() throws Exception
    {
        ConnectionDefinitionData satelliteConDfn = ConnectionDefinitionData.getInstance(
            sysCtx,
            resDfn,
            nodeSrc,
            nodeDst,
            conNr,
            null,
            null,
            false
        );

        assertNull(satelliteConDfn);

        PreparedStatement stmt = con.prepareStatement(SELECT_ALL_CON_DFNS);
        ResultSet resultSet = stmt.executeQuery();

        assertFalse(resultSet.next());
        resultSet.close();
        stmt.close();
    }
}
