package com.linbit.linstor;

import com.google.inject.Inject;
import com.linbit.InvalidNameException;
import com.linbit.TransactionMgr;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.DerbyBase;
import com.linbit.utils.UuidUtils;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

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
    @Inject private NodeConnectionDataDerbyDriver driver;

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

        nodeSrc = nodeDataFactory.getInstance(SYS_CTX, sourceName, null, null, transMgr, true, false);
        nodesMap.put(nodeSrc.getName(), nodeSrc);
        nodeDst = nodeDataFactory.getInstance(SYS_CTX, targetName, null, null, transMgr, true, false);
        nodesMap.put(nodeDst.getName(), nodeDst);

        nodeCon = new NodeConnectionData(uuid, SYS_CTX, nodeSrc, nodeDst, driver, transMgr, propsContainerFactory);
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
        nodeConnectionDataFactory.getInstance(SYS_CTX, nodeSrc, nodeDst, transMgr, true, false);

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

        NodeConnectionData loadedConDfn = nodeConnectionDataFactory.getInstance(
            SYS_CTX,
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
        NodeConnectionData storedInstance = nodeConnectionDataFactory.getInstance(
            SYS_CTX,
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
        NodeConnectionData satelliteConDfn = nodeConnectionDataFactory.getInstance(
            SYS_CTX,
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

        NodeName sourceName2 = new NodeName("OtherSourceNode");
        NodeName targetName2 = new NodeName("OtherTargetNode");
        NodeData nodeSrc2 = nodeDataFactory.getInstance(SYS_CTX, sourceName2, null, null, transMgr, true, false);
        NodeData nodeDst2 = nodeDataFactory.getInstance(SYS_CTX, targetName2, null, null, transMgr, true, false);

        NodeConnectionData satelliteConDfn = nodeConnectionDataFactory.getInstance(
            SYS_CTX,
            nodeSrc2,
            nodeDst2,
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

    @Test (expected = LinStorDataAlreadyExistsException.class)
    public void testAlreadyExists() throws Exception
    {
        driver.create(nodeCon, transMgr);

        nodeConnectionDataFactory.getInstance(SYS_CTX, nodeSrc, nodeDst, transMgr, false, true);
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
        Node sourceNode = loadedConDfn.getSourceNode(SYS_CTX);
        Node targetNode = loadedConDfn.getTargetNode(SYS_CTX);

        assertEquals(sourceName, sourceNode.getName());
        assertEquals(targetName, targetNode.getName());
    }
}
