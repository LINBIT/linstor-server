package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.StorPool.InitMaps;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.dbdrivers.GenericDbDriver;
import com.linbit.linstor.dbdrivers.derby.DbConstants;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDataDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@Singleton
public class StorPoolDataGenericDbDriver implements StorPoolDataDatabaseDriver
{
    private static final String TBL_NSP = DbConstants.TBL_NODE_STOR_POOL;

    private static final String SP_UUID = DbConstants.UUID;
    private static final String SP_NODE = DbConstants.NODE_NAME;
    private static final String SP_POOL = DbConstants.POOL_NAME;
    private static final String SP_DRIVER = DbConstants.DRIVER_NAME;

    private static final String SELECT_ALL =
        " SElECT " + SP_UUID + ", " + SP_NODE + ", " + SP_POOL + ", " + SP_DRIVER +
        " FROM " + TBL_NSP;
    private static final String SP_SELECT_BY_NODE =
        SELECT_ALL +
        " WHERE " + SP_NODE + " = ?";
    private static final String SP_SELECT_BY_NODE_AND_SP =
        SP_SELECT_BY_NODE +
        " AND "  + SP_POOL + " = ?";

    private static final String SP_INSERT =
        " INSERT INTO " + TBL_NSP +
        " (" + SP_UUID + ", " + SP_NODE + ", " + SP_POOL + ", " + SP_DRIVER + ")" +
        " VALUES (?, ?, ?, ?)";
    private static final String SP_DELETE =
        " DELETE FROM " + TBL_NSP +
        " WHERE " + SP_NODE + " = ? AND " +
        "       " + SP_POOL + " = ?";

    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;

    private final Map<String, StorPoolData> storPoolCache = new HashMap<>();

    private boolean cacheCleared = false;

    private final Provider<NodeDataGenericDbDriver> nodeDriverProvider;
    private final Provider<StorPoolDefinitionDataGenericDbDriver> storPoolDefDriverProvider;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public StorPoolDataGenericDbDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        Provider<NodeDataGenericDbDriver> nodeDriverProviderRef,
        Provider<StorPoolDefinitionDataGenericDbDriver> storPoolDefDriverProviderRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        dbCtx = dbCtxRef;
        errorReporter = errorReporterRef;
        nodeDriverProvider = nodeDriverProviderRef;
        storPoolDefDriverProvider = storPoolDefDriverProviderRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
    }

    @Override
    @SuppressWarnings("checkstyle:magicnumber")
    public void create(StorPoolData storPoolData) throws SQLException
    {
        errorReporter.logTrace("Creating StorPool %s", getId(storPoolData));
        try (PreparedStatement stmt = getConnection().prepareStatement(SP_INSERT))
        {
            stmt.setString(1, storPoolData.getUuid().toString());
            stmt.setString(2, storPoolData.getNode().getName().value);
            stmt.setString(3, storPoolData.getName().value);
            stmt.setString(4, storPoolData.getDriverName());
            stmt.executeUpdate();
        }
        errorReporter.logTrace("StorPool created %s", getId(storPoolData));
    }

    @Override
    public StorPoolData load(Node node, StorPoolDefinition storPoolDfn, boolean logWarnIfNotExists)
        throws SQLException
    {
        errorReporter.logTrace("Loading StorPool %s", getId(node, storPoolDfn));
        StorPoolData sp = cacheGet(node, storPoolDfn);
        if (sp == null)
        {
            try (PreparedStatement stmt = getConnection().prepareStatement(SP_SELECT_BY_NODE_AND_SP))
            {
                stmt.setString(1, node.getName().value);
                stmt.setString(2, storPoolDfn.getName().value);
                try (ResultSet resultSet = stmt.executeQuery())
                {
                    List<StorPoolData> list = restore(node, resultSet);
                    if (list.isEmpty())
                    {
                        if (logWarnIfNotExists)
                        {
                            errorReporter.logWarning(
                                "StorPool was not found in the DB %s",
                                getId(node, storPoolDfn)
                            );
                        }
                    }
                    else
                    if (list.size() != 1)
                    {
                        throw new ImplementationError("expected single result query returned multiple objects", null);
                    }
                    else
                    {
                        sp = list.get(0);
                        errorReporter.logTrace("StorPool loaded from DB", getId(sp));
                    }
                }
            }
        }
        else
        {
            errorReporter.logTrace("StorPool loaded from cache %s", getId(sp));
        }
        return sp;
    }

    public Map<StorPoolData, InitMaps> loadAll(
        Map<NodeName, ? extends Node> nodesMap,
        Map<StorPoolName, ? extends StorPoolDefinition> storPoolDfnMap
    )
        throws SQLException
    {
        Map<StorPoolData, StorPool.InitMaps> storPools = new TreeMap<>();
        errorReporter.logTrace("Loading all Storage Pool");
        try (PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL))
        {
            try (ResultSet resultSet = stmt.executeQuery())
            {
                while (resultSet.next())
                {
                    try
                    {
                        NodeName nodeName = new NodeName(resultSet.getString(SP_NODE));
                        StorPoolName storPoolName = new StorPoolName(resultSet.getString(SP_POOL));

                        Pair<StorPoolData, InitMaps> pair = restoreStorPool(
                            resultSet,
                            nodesMap.get(nodeName),
                            storPoolDfnMap.get(storPoolName)
                        );
                        storPools.put(
                            pair.objA,
                            pair.objB
                        );
                    }
                    catch (InvalidNameException exc)
                    {
                        throw new ImplementationError(
                            "Invalid name restored from database: " + exc.invalidName,
                            exc
                        );
                    }
                }
            }
        }
        errorReporter.logTrace("Loaded %d Storage Pools", storPools.size());
        return storPools;
    }

    private Pair<StorPoolData, InitMaps> restoreStorPool(
        ResultSet resultSet,
        Node node,
        StorPoolDefinition storPoolDfn
    )
        throws SQLException
    {
        Map<String, Volume> vlmMap = new TreeMap<>();
        StorPoolData storPool = new StorPoolData(
            java.util.UUID.fromString(resultSet.getString(SP_UUID)),
            node,
            storPoolDfn,
            resultSet.getString(SP_DRIVER),
            false,
            this,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider,
            vlmMap
        );
        if (storPool.getName().displayValue.equals(LinStor.DISKLESS_STOR_POOL_NAME) &&
            node instanceof NodeData)
        {
            ((NodeData) node).setDisklessStorPool(storPool);
        }
        return new Pair<>(storPool, new StorPoolInitMaps(vlmMap));
    }

    public List<StorPoolData> loadStorPools(NodeData node)
        throws SQLException
    {
        errorReporter.logTrace("Loading all StorPools for Node (NodeName=%s)", node.getName().value);
        List<StorPoolData> storPoolList = new ArrayList<>();
        try (PreparedStatement stmt = getConnection().prepareStatement(SP_SELECT_BY_NODE))
        {
            stmt.setString(1, node.getName().value);

            try (ResultSet resultSet = stmt.executeQuery())
            {
                storPoolList = restore(node, resultSet);
            }
        }
        errorReporter.logTrace(
            "%d StorPools loaded for Node (NodeName=%s)",
            storPoolList.size(),
            node.getName().displayValue
        );
        return storPoolList;
    }

    private List<StorPoolData> restore(Node nodeRef, ResultSet resultSet)
        throws SQLException
    {
        List<StorPoolData> storPoolList = new ArrayList<>();
        while (resultSet.next())
        {
            StorPoolName storPoolName;
            try
            {
                storPoolName = new StorPoolName(resultSet.getString(SP_POOL));
            }
            catch (InvalidNameException invalidNameExc)
            {
                throw new LinStorSqlRuntimeException(
                    String.format(
                        "A StorPoolName of a stored StorPool in the table %s could not be restored. " +
                            "(NodeName=%s, invalid StorPoolName=%s)",
                        TBL_NSP,
                        resultSet.getString(SP_NODE),
                        resultSet.getString(SP_POOL)
                    ),
                    invalidNameExc
                );
            }

            Node node;
            if (nodeRef != null)
            {
                node = nodeRef;
            }
            else
            {
                NodeName nodeName;
                try
                {
                    nodeName = new NodeName(resultSet.getString(SP_NODE));
                }
                catch (InvalidNameException invalidNameExc)
                {
                    throw new LinStorSqlRuntimeException(
                        String.format(
                            "A NodeName of a stored StorPool in the table %s could not be restored. " +
                                "(invalid NodeName=%s, StorPoolName=%s)",
                            TBL_NSP,
                            resultSet.getString(SP_NODE),
                            resultSet.getString(SP_POOL)
                        ),
                        invalidNameExc
                    );
                }
                node = nodeDriverProvider.get().load(nodeName, true);
            }

            StorPoolData storPoolData = cacheGet(node, storPoolName);
            if (storPoolData == null)
            {
                StorPoolDefinitionData storPoolDef = storPoolDefDriverProvider.get().load(
                    storPoolName,
                    true
                );
                storPoolData = restoreStorPool(resultSet, node, storPoolDef).objA;
                errorReporter.logTrace("Loaded StorPool from DB %s", getId(storPoolData));
            }
            else
            {
                errorReporter.logTrace("Loaded StorPool from cache %s", getId(storPoolData));
            }
            storPoolList.add(storPoolData);
        }
        return storPoolList;
    }

    @Override
    public void delete(StorPoolData storPool) throws SQLException
    {
        errorReporter.logTrace("Deleting StorPool %s", getId(storPool));
        try (PreparedStatement stmt = getConnection().prepareStatement(SP_DELETE))
        {
            Node node = storPool.getNode();
            StorPoolDefinition storPoolDfn = storPool.getDefinition(dbCtx);

            stmt.setString(1, node.getName().value);
            stmt.setString(2, storPoolDfn.getName().value);

            stmt.executeUpdate();
            errorReporter.logTrace("StorPool deleted %s", getId(storPool));
        }
        catch (AccessDeniedException accDeniedExc)
        {
            GenericDbDriver.handleAccessDeniedException(accDeniedExc);
        }
    }

    @Override
    public void ensureEntryExists(StorPoolData storPoolData) throws SQLException
    {
        errorReporter.logTrace("Ensuring StorPool exists %s", getId(storPoolData));
        Node node = storPoolData.getNode();
        StorPoolDefinition storPoolDfn;
        try (PreparedStatement stmt = getConnection().prepareStatement(SP_SELECT_BY_NODE_AND_SP);)
        {
            storPoolDfn = storPoolData.getDefinition(dbCtx);

            stmt.setString(1, node.getName().value);
            stmt.setString(2, storPoolDfn.getName().value);

            try (ResultSet resultSet = stmt.executeQuery())
            {
                if (!resultSet.next())
                {
                    create(storPoolData);
                }
                else
                {
                    errorReporter.logTrace("StorPool existed, nothing to do %s", getId(storPoolData));
                }
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            GenericDbDriver.handleAccessDeniedException(accDeniedExc);
        }
    }

    private StorPoolData cacheGet(Node node, StorPoolDefinition storPoolDfn)
    {
        return cacheGet(node, storPoolDfn.getName());
    }

    private StorPoolData cacheGet(Node node, StorPoolName storPoolName)
    {
        // first, ask the node
        StorPoolData ret = null;
        try
        {
            ret = (StorPoolData) node.getStorPool(dbCtx, storPoolName);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            GenericDbDriver.handleAccessDeniedException(accDeniedExc);
        }
        if (ret == null && !cacheCleared)
        {
            // second, as our internal cache. This is needed as we would otherwise
            // endless-recursive call our load with volume's load
            ret = storPoolCache.get(
                getId(
                    node.getName().value,
                    storPoolName.value
                )
            );
        }
        return ret;
    }

    private Connection getConnection()
    {
        return transMgrProvider.get().getConnection();
    }

    private String getId(StorPoolData storPoolData)
    {
        return getId(
            storPoolData.getNode().getName().displayValue,
            storPoolData.getName().displayValue
        );
    }

    private String getId(Node node, StorPoolDefinition storPoolDfn)
    {
        return getId(
            node.getName().displayValue,
            storPoolDfn.getName().displayValue
        );
    }

    private String getId(String nodeName, String poolName)
    {
        return "(NodeName=" + nodeName + " PoolName=" + poolName + ")";
    }

    public void clearCache()
    {
        cacheCleared = true;
        storPoolCache.clear();
    }

    private class StorPoolInitMaps implements StorPool.InitMaps
    {
        private final Map<String, Volume> vlmMap;

        StorPoolInitMaps(Map<String, Volume> vlmMapRef)
        {
            vlmMap = vlmMapRef;
        }

        @Override
        public Map<String, Volume> getVolumeMap()
        {
            return vlmMap;
        }
    }
}
