package com.linbit.drbdmanage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import org.junit.Test;

import com.linbit.InvalidNameException;
import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.core.DrbdManage;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.DerbyBase;
import com.linbit.utils.UuidUtils;

public class NodeConnectionDataDerbyTest extends DerbyBase
{
    private static final String SELECT_ALL_RES_CON_DFNS =
        " SELECT " + UUID + ", " + NODE_NAME_SRC + ", " + NODE_NAME_DST +
        " FROM " + TBL_NODE_CONNECTIONS;

    private final NodeName sourceName;
    private final NodeName targetName;

    private TransactionMgr transMgr;

    private java.util.UUID uuid;
    private NodeData nodeSrc;
    private NodeData nodeDst;

    private NodeConnectionData nodeCon;
    private NodeConnectionDataDerbyDriver driver;

    public NodeConnectionDataDerbyTest() throws InvalidNameException
    {
        sourceName = new NodeName("testNodeSource");
        targetName = new NodeName("testNodeTarget");
    }

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        assertEquals(TBL_NODE_CONNECTIONS + " table's column count has changed. Update tests accordingly!", 3, TBL_COL_COUNT_NODE_CONNECTIONS);

        transMgr = new TransactionMgr(getConnection());

        uuid = randomUUID();

        nodeSrc = NodeData.getInstance(sysCtx, sourceName, null, null, transMgr, true, false);
        nodeDst = NodeData.getInstance(sysCtx, targetName, null, null, transMgr, true, false);

        nodeCon = new NodeConnectionData(uuid, nodeSrc, nodeDst, transMgr);
        driver = (NodeConnectionDataDerbyDriver) DrbdManage.getNodeConnectionDatabaseDriver();
    }

    @Test
    public void testPersist() throws Exception
    {
        driver.create(nodeCon, transMgr);

        checkDbPersist(true);
    }

    @Test
    public void testPersistGetInstance() throws Exception
    {
        NodeConnectionData.getInstance(sysCtx, nodeSrc, nodeDst, transMgr, true, false);

        checkDbPersist(false);
    }

    @Test
    public void testLoad() throws Exception
    {
        driver.create(nodeCon, transMgr);

        NodeConnectionData loadedConDfn = driver.load(nodeSrc , nodeDst, true, transMgr);

        checkLoadedConDfn(loadedConDfn, true);
    }

    @Test
    public void testLoadAll() throws Exception
    {
        driver.create(nodeCon, transMgr);

        List<NodeConnectionData> cons = driver.loadAllByNode(nodeSrc, transMgr);

        assertNotNull(cons);

        assertEquals(1, cons.size());

        NodeConnection loadedConDfn = cons.get(0);
        assertNotNull(loadedConDfn);

        checkLoadedConDfn(loadedConDfn, true);
    }

    @Test
    public void testLoadGetInstance() throws Exception
    {
        driver.create(nodeCon, transMgr);

        NodeConnectionData loadedConDfn = NodeConnectionData.getInstance(
            sysCtx,
            nodeSrc,
            nodeDst,
            transMgr,
            false,
            false
        );

        checkLoadedConDfn(loadedConDfn, true);
    }

    @Test
    public void testCache() throws Exception
    {
        NodeConnectionData storedInstance = NodeConnectionData.getInstance(
            sysCtx,
            nodeSrc,
            nodeDst,
            transMgr,
            true,
            false
        );

        // no clear-cache

        assertEquals(storedInstance, driver.load(nodeSrc, nodeDst, true, transMgr));
    }

    @Test
    public void testDelete() throws Exception
    {
        driver.create(nodeCon, transMgr);

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_RES_CON_DFNS);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        assertFalse(resultSet.next());
        resultSet.close();

        driver.delete(nodeCon, transMgr);

        resultSet = stmt.executeQuery();

        assertFalse(resultSet.next());
        resultSet.close();

        stmt.close();
    }

    @Test
    public void testSatelliteCreate() throws Exception
    {
        satelliteMode();
        NodeConnectionData satelliteConDfn = NodeConnectionData.getInstance(
            sysCtx,
            nodeSrc,
            nodeDst,
            null,
            true,
            false
        );

        checkLoadedConDfn(satelliteConDfn, false);

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_RES_CON_DFNS);
        ResultSet resultSet = stmt.executeQuery();

        assertFalse(resultSet.next());
        resultSet.close();
        stmt.close();
    }

    @Test
    public void testSatelliteNoCreate() throws Exception
    {
        satelliteMode();
        NodeConnectionData satelliteConDfn = NodeConnectionData.getInstance(
            sysCtx,
            nodeSrc,
            nodeDst,
            null,
            false,
            false
        );

        assertNull(satelliteConDfn);

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_RES_CON_DFNS);
        ResultSet resultSet = stmt.executeQuery();

        assertFalse(resultSet.next());
        resultSet.close();
        stmt.close();
    }

    @Test (expected = DrbdDataAlreadyExistsException.class)
    public void testAlreadyExists() throws Exception
    {
        driver.create(nodeCon, transMgr);

        NodeConnectionData.getInstance(sysCtx, nodeSrc, nodeDst, transMgr, false, true);
    }


    private void checkDbPersist(boolean checkUuid) throws SQLException
    {
        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SELECT_ALL_RES_CON_DFNS);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        if (checkUuid)
        {
            assertEquals(uuid, UuidUtils.asUuid(resultSet.getBytes(UUID)));
        }
        assertEquals(sourceName.value, resultSet.getString(NODE_NAME_SRC));
        assertEquals(targetName.value, resultSet.getString(NODE_NAME_DST));

        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    private void checkLoadedConDfn(NodeConnection loadedConDfn, boolean checkUuid) throws AccessDeniedException
    {
        assertNotNull(loadedConDfn);
        if (checkUuid)
        {
            assertEquals(uuid, loadedConDfn.getUuid());
        }
        Node sourceNode = loadedConDfn.getSourceNode(sysCtx);
        Node targetNode = loadedConDfn.getTargetNode(sysCtx);

        assertEquals(sourceName, sourceNode.getName());
        assertEquals(targetName, targetNode.getName());
    }
}
