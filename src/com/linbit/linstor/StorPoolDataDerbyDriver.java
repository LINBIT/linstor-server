package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.TransactionMgr;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.dbdrivers.DerbyDriver;
import com.linbit.linstor.dbdrivers.derby.DerbyConstants;
import com.linbit.linstor.dbdrivers.interfaces.NodeDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDefinitionDataDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.utils.UuidUtils;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Singleton
public class StorPoolDataDerbyDriver implements StorPoolDataDatabaseDriver
{
    private static final String TBL_NSP = DerbyConstants.TBL_NODE_STOR_POOL;

    private static final String SP_UUID = DerbyConstants.UUID;
    private static final String SP_NODE = DerbyConstants.NODE_NAME;
    private static final String SP_POOL = DerbyConstants.POOL_NAME;
    private static final String SP_DRIVER = DerbyConstants.DRIVER_NAME;

    private static final String SP_SELECT_BY_NODE =
        " SElECT " + SP_UUID + ", " + SP_NODE + ", " + SP_POOL + ", " + SP_DRIVER +
        " FROM " + TBL_NSP +
        " WHERE " + SP_NODE + " = ?";
    private static final String SP_SELECT_BY_NODE_AND_SP = SP_SELECT_BY_NODE +
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

    private final Provider<VolumeDataDerbyDriver> volumeDriverProvider;

    @Inject
    public StorPoolDataDerbyDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        Provider<VolumeDataDerbyDriver> volumeDriverProviderRef
    )
    {
        dbCtx = dbCtxRef;
        errorReporter = errorReporterRef;
        volumeDriverProvider = volumeDriverProviderRef;
    }

    @Override
    public void create(StorPoolData storPoolData, TransactionMgr transMgr) throws SQLException
    {
        errorReporter.logTrace("Creating StorPool %s", getId(storPoolData));
        try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(SP_INSERT))
        {
            stmt.setBytes(1, UuidUtils.asByteArray(storPoolData.getUuid()));
            stmt.setString(2, storPoolData.getNode().getName().value);
            stmt.setString(3, storPoolData.getName().value);
            stmt.setString(4, storPoolData.getDriverName());
            stmt.executeUpdate();
        }
        errorReporter.logTrace("StorPool created %s", getId(storPoolData));
    }

    @Override
    public StorPoolData load(
        Node node,
        StorPoolDefinition storPoolDfn,
        boolean logWarnIfNotExists,
        TransactionMgr transMgr
    )
        throws SQLException
    {
        errorReporter.logTrace("Loading StorPool %s", getId(node, storPoolDfn));
        StorPoolData sp = cacheGet(node, storPoolDfn);
        if (sp == null)
        {
            try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(SP_SELECT_BY_NODE_AND_SP))
            {
                stmt.setString(1, node.getName().value);
                stmt.setString(2, storPoolDfn.getName().value);
                try (ResultSet resultSet = stmt.executeQuery())
                {
                    List<StorPoolData> list = restore(node, resultSet, transMgr);
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

    public List<StorPoolData> loadStorPools(
        NodeData node,
        TransactionMgr transMgr
    )
        throws SQLException
    {
        errorReporter.logTrace("Loading all StorPools for Node (NodeName=%s)", node.getName().value);
        List<StorPoolData> storPoolList = new ArrayList<>();
        try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(SP_SELECT_BY_NODE))
        {
            stmt.setString(1, node.getName().value);

            try (ResultSet resultSet = stmt.executeQuery())
            {
                storPoolList = restore(node, resultSet, transMgr);
            }
        }
        errorReporter.logTrace(
            "%d StorPools loaded for Node (NodeName=%s)",
            storPoolList.size(),
            node.getName().displayValue
        );
        return storPoolList;
    }

    private List<StorPoolData> restore(
        Node nodeRef,
        ResultSet resultSet,
        TransactionMgr transMgr
    )
        throws SQLException
    {
        List<StorPoolData> storPoolList = new ArrayList<>();
        NodeDataDatabaseDriver nodeDriver = LinStor.getNodeDataDatabaseDriver();
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
                node = nodeDriver.load(nodeName, true, transMgr);
            }

            StorPoolData storPoolData = cacheGet(node, storPoolName);
            if (storPoolData == null)
            {
                StorPoolDefinitionDataDatabaseDriver storPoolDefDriver = LinStor.getStorPoolDefinitionDataDatabaseDriver();
                StorPoolDefinitionData storPoolDef = storPoolDefDriver.load(
                    storPoolName,
                    true,
                    transMgr
                );

                try
                {
                    storPoolData = new StorPoolData(
                        UuidUtils.asUuid(resultSet.getBytes(SP_UUID)),
                        dbCtx,
                        node,
                        storPoolDef,
                        resultSet.getString(SP_DRIVER),
                        // controller should not have an instance of storage driver.
                        false,
                        transMgr
                    );
                }
                catch (AccessDeniedException accDeniedExc)
                {
                    DerbyDriver.handleAccessDeniedException(accDeniedExc);
                }

                if (!cacheCleared)
                {
                    storPoolCache.put(
                        getId(
                            node.getName().value,
                            storPoolName.value
                        ),
                        storPoolData
                    );
                }

                // restore volumes
                List<VolumeData> volumes = volumeDriverProvider.get().getVolumesByStorPool(
                    storPoolData,
                    transMgr
                );
                try
                {
                    for (VolumeData vol : volumes)
                    {
                        storPoolData.putVolume(dbCtx, vol);
                    }
                }
                catch (AccessDeniedException accDeniedExc)
                {
                    DerbyDriver.handleAccessDeniedException(accDeniedExc);
                }
                errorReporter.logTrace("%d Volumes restored for StorPool %s",
                    volumes.size(),
                    getId(storPoolData)
                );

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
    public void delete(StorPoolData storPool, TransactionMgr transMgr) throws SQLException
    {
        errorReporter.logTrace("Deleting StorPool %s", getId(storPool));
        try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(SP_DELETE))
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
            DerbyDriver.handleAccessDeniedException(accDeniedExc);
        }
    }

    @Override
    public void ensureEntryExists(StorPoolData storPoolData, TransactionMgr transMgr) throws SQLException
    {
        errorReporter.logTrace("Ensuring StorPool exists %s", getId(storPoolData));
        Node node = storPoolData.getNode();
        StorPoolDefinition storPoolDfn;
        try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(SP_SELECT_BY_NODE_AND_SP);)
        {
            storPoolDfn = storPoolData.getDefinition(dbCtx);

            stmt.setString(1, node.getName().value);
            stmt.setString(2, storPoolDfn.getName().value);

            try (ResultSet resultSet = stmt.executeQuery())
            {
                if (!resultSet.next())
                {
                    create(storPoolData, transMgr);
                }
                else
                {
                    errorReporter.logTrace("StorPool existed, nothing to do %s", getId(storPoolData));
                }
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            DerbyDriver.handleAccessDeniedException(accDeniedExc);
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
            DerbyDriver.handleAccessDeniedException(accDeniedExc);
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
}
