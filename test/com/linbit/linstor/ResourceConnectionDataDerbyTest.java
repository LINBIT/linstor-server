package com.linbit.linstor;

import javax.inject.Inject;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.ResourceDefinition.TransportType;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.DerbyBase;
import com.linbit.utils.UuidUtils;
import org.junit.Before;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ResourceConnectionDataDerbyTest extends DerbyBase
{
    private static final String SELECT_ALL_RES_CON_DFNS =
        " SELECT " + UUID + ", " + NODE_NAME_SRC + ", " +
                     NODE_NAME_DST + ", " + RESOURCE_NAME +
        " FROM " + TBL_RESOURCE_CONNECTIONS;

    private final ResourceName resName;
    private final Integer resPort;
    private final NodeName sourceName;
    private final NodeName targetName;

    private java.util.UUID uuid;
    private ResourceDefinitionData resDfn;
    private NodeData nodeSrc;
    private NodeData nodeDst;

    private ResourceConnectionData resCon;

    @Inject private ResourceConnectionDataDerbyDriver driver;

    private NodeId nodeIdSrc;
    private NodeId nodeIdDst;

    private ResourceData resSrc;
    private ResourceData resDst;

    @SuppressWarnings("checkstyle:magicnumber")
    public ResourceConnectionDataDerbyTest() throws InvalidNameException, ValueOutOfRangeException
    {
        resName = new ResourceName("testResourceName");
        resPort = 9001;
        sourceName = new NodeName("testNodeSource");
        targetName = new NodeName("testNodeTarget");
    }

    @SuppressWarnings("checkstyle:magicnumber")
    @Before
    public void setUp() throws Exception
    {
        super.setUpAndEnterScope();
        assertEquals(
            TBL_RESOURCE_CONNECTIONS + " table's column count has changed. Update tests accordingly!",
            4,
            TBL_COL_COUNT_RESOURCE_CONNECTIONS
        );

        uuid = randomUUID();

        resDfn = resourceDefinitionDataFactory.create(
            SYS_CTX, resName, resPort, null, "secret", TransportType.IP
        );
        rscDfnMap.put(resDfn.getName(), resDfn);
        nodeSrc = nodeDataFactory.getInstance(SYS_CTX, sourceName, null, null, true, false);
        nodeDst = nodeDataFactory.getInstance(SYS_CTX, targetName, null, null, true, false);

        nodeIdSrc = new NodeId(13);
        nodeIdDst = new NodeId(14);

        resSrc = resourceDataFactory.getInstance(SYS_CTX, resDfn, nodeSrc, nodeIdSrc, null, true, false);
        resDst = resourceDataFactory.getInstance(SYS_CTX, resDfn, nodeDst, nodeIdDst, null, true, false);

        resCon = new ResourceConnectionData(
            uuid,
            SYS_CTX,
            resSrc,
            resDst,
            driver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider
        );
    }

    @Test
    public void testPersist() throws Exception
    {
        driver.create(resCon);
        commit();

        checkDbPersist(true);
    }

    @Test
    public void testPersistGetInstance() throws Exception
    {
        resourceConnectionDataFactory.getInstance(SYS_CTX, resSrc, resDst, true, false);
        commit();

        checkDbPersist(false);
    }

    @Test
    public void testLoad() throws Exception
    {
        driver.create(resCon);

        ResourceConnectionData loadedConDfn = driver.load(resSrc, resDst, true);

        checkLoadedConDfn(loadedConDfn, true);
    }

    @Test
    public void testLoadAll() throws Exception
    {
        driver.create(resCon);

        List<ResourceConnectionData> cons = driver.loadAllByResource(resSrc);

        assertNotNull(cons);

        assertEquals(1, cons.size());

        ResourceConnection loadedConDfn = cons.get(0);
        assertNotNull(loadedConDfn);

        checkLoadedConDfn(loadedConDfn, true);
    }

    @Test
    public void testLoadGetInstance() throws Exception
    {
        driver.create(resCon);

        ResourceConnectionData loadedConDfn = resourceConnectionDataFactory.getInstance(
            SYS_CTX,
            resSrc,
            resDst,
            false,
            false
        );

        checkLoadedConDfn(loadedConDfn, true);
    }

    @Test
    public void testCache() throws Exception
    {
        ResourceConnectionData storedInstance = resourceConnectionDataFactory.getInstance(
            SYS_CTX,
            resSrc,
            resDst,
            true,
            false
        );

        // no clear-cache

        assertEquals(storedInstance, driver.load(resSrc, resDst, true));
    }

    @Test
    public void testDelete() throws Exception
    {
        driver.create(resCon);
        commit();

        PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_RES_CON_DFNS);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        assertFalse(resultSet.next());
        resultSet.close();

        driver.delete(resCon);

        resultSet = stmt.executeQuery();

        assertFalse(resultSet.next());
        resultSet.close();

        stmt.close();
    }

    private void checkDbPersist(boolean checkUuid) throws SQLException
    {
        PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_RES_CON_DFNS);
        ResultSet resultSet = stmt.executeQuery();

        assertTrue(resultSet.next());
        if (checkUuid)
        {
            assertEquals(uuid, java.util.UUID.fromString(resultSet.getString(UUID)));
        }
        assertEquals(resName.value, resultSet.getString(RESOURCE_NAME));
        assertEquals(sourceName.value, resultSet.getString(NODE_NAME_SRC));
        assertEquals(targetName.value, resultSet.getString(NODE_NAME_DST));

        assertFalse(resultSet.next());

        resultSet.close();
        stmt.close();
    }

    private void checkLoadedConDfn(ResourceConnection loadedConDfn, boolean checkUuid) throws AccessDeniedException
    {
        assertNotNull(loadedConDfn);
        if (checkUuid)
        {
            assertEquals(uuid, loadedConDfn.getUuid());
        }
        Resource sourceResource = loadedConDfn.getSourceResource(SYS_CTX);
        Resource targetResource = loadedConDfn.getTargetResource(SYS_CTX);

        assertEquals(resName, sourceResource.getDefinition().getName());
        assertEquals(sourceName, sourceResource.getAssignedNode().getName());
        assertEquals(targetName, targetResource.getAssignedNode().getName());
        assertEquals(sourceResource.getDefinition().getName(), targetResource.getDefinition().getName());
    }

    @Test (expected = LinStorDataAlreadyExistsException.class)
    public void testAlreadyExists() throws Exception
    {
        driver.create(resCon);

        resourceConnectionDataFactory.getInstance(SYS_CTX, resSrc, resDst, false, true);
    }
}
