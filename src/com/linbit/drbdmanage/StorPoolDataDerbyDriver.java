package com.linbit.drbdmanage;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.core.DrbdManage;
import com.linbit.drbdmanage.dbdrivers.PrimaryKey;
import com.linbit.drbdmanage.dbdrivers.derby.DerbyConstants;
import com.linbit.drbdmanage.dbdrivers.interfaces.StorPoolDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.StorPoolDefinitionDataDatabaseDriver;
import com.linbit.drbdmanage.logging.ErrorReporter;
import com.linbit.drbdmanage.propscon.SerialGenerator;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.ObjectProtection;
import com.linbit.drbdmanage.security.ObjectProtectionDatabaseDriver;
import com.linbit.utils.UuidUtils;

public class StorPoolDataDerbyDriver implements StorPoolDataDatabaseDriver
{
    private static final String TBL_NSP = DerbyConstants.TBL_NODE_STOR_POOL;

    private static final String NSP_UUID = DerbyConstants.UUID;
    private static final String NSP_NODE = DerbyConstants.NODE_NAME;
    private static final String NSP_POOL = DerbyConstants.POOL_NAME;
    private static final String NSP_DRIVER = DerbyConstants.DRIVER_NAME;

    private static final String NSP_SELECT_BY_NODE =
        " SElECT " + NSP_UUID + ", " + NSP_NODE + ", " + NSP_POOL + ", " + NSP_DRIVER +
        " FROM " + TBL_NSP +
        " WHERE " + NSP_NODE + " = ?";
    private static final String NSP_SELECT = NSP_SELECT_BY_NODE +
        " AND "  + NSP_POOL + " = ?";
    private static final String NSP_INSERT =
        " INSERT INTO " + TBL_NSP +
        " VALUES (?, ?, ?, ?)";
    private static final String NSP_DELETE =
        " DELETE FROM " + TBL_NSP +
        " WHERE " + NSP_NODE + " = ? AND " +
        "       " + NSP_POOL + " = ?";


    private static Hashtable<PrimaryKey, StorPoolData> storPoolCache = new Hashtable<>();

    private final ErrorReporter errorReporter;
    private final AccessContext dbCtx;

    public StorPoolDataDerbyDriver(AccessContext dbCtxRef, ErrorReporter errorReporterRef)
    {
        dbCtx = dbCtxRef;
        errorReporter = errorReporterRef;
    }

    @Override
    public void create(StorPoolData storPoolData, TransactionMgr transMgr) throws SQLException
    {
        errorReporter.logDebug(
            "Creating StorPoolData (NodeName=%s, PoolName=%s)",
            storPoolData.getNode().getName().value,
            storPoolData.getName().value
        );

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(NSP_INSERT);
        stmt.setBytes(1, UuidUtils.asByteArray(storPoolData.getUuid()));
        stmt.setString(2, storPoolData.getNode().getName().value);
        stmt.setString(3, storPoolData.getName().value);
        stmt.setString(4, storPoolData.getDriverName());
        stmt.executeUpdate();
        stmt.close();

        cache(storPoolData.getNode(), storPoolData);
    }

    @Override
    public StorPoolData load(
        Node node,
        StorPoolDefinition storPoolDefinition,
        SerialGenerator serialGen,
        TransactionMgr transMgr
    )
        throws SQLException
    {
        PreparedStatement stmt = transMgr.dbCon.prepareStatement(NSP_SELECT);
        stmt.setString(1, node.getName().value);
        stmt.setString(2, storPoolDefinition.getName().value);
        ResultSet resultSet = stmt.executeQuery();

        StorPoolData sp = cacheGet(node, storPoolDefinition);
        if (sp == null)
        {
            if (resultSet.next())
            {
                ObjectProtectionDatabaseDriver objProtDriver = DrbdManage.getObjectProtectionDatabaseDriver();
                ObjectProtection objProt = objProtDriver.loadObjectProtection(
                    ObjectProtection.buildPathSP(
                        storPoolDefinition.getName()
                    ),
                    transMgr
                );

                sp = new StorPoolData(
                    UuidUtils.asUuid(resultSet.getBytes(NSP_UUID)),
                    objProt,
                    node,
                    storPoolDefinition,
                    null,   // storageDriver, has to be null in the controller
                    resultSet.getString(NSP_DRIVER),
                    serialGen,
                    transMgr
                );
                if (!cache(node, sp))
                {
                    sp = cacheGet(node, storPoolDefinition);
                }
                else
                {
                    // restore references
                }
            }
        }
        else
        {
            // we have a cached storPool
            if (!resultSet.next())
            {
                // but no entry in the db...

                // XXX: user deleted db entry during runtime - throw exception?
                // or just remove the item from the cache + detach item from parent (if needed) + warn the user?
            }
        }
        resultSet.close();
        stmt.close();

        return sp;
    }

    public static List<StorPoolData> loadStorPools(
        NodeData node,
        SerialGenerator serGen,
        TransactionMgr transMgr
    )
        throws SQLException
    {
        PreparedStatement stmt = transMgr.dbCon.prepareStatement(NSP_SELECT_BY_NODE);
        stmt.setString(1, node.getName().value);
        ResultSet resultSet = stmt.executeQuery();

        List<StorPoolData> storPoolList = new ArrayList<>();
        try
        {
            while(resultSet.next())
            {
                StorPoolName storPoolName = new StorPoolName(resultSet.getString(NSP_POOL));

                StorPoolData storPoolData = cacheGet(node, storPoolName);
                if (storPoolData == null)
                {
                    ObjectProtectionDatabaseDriver objProtDriver = DrbdManage.getObjectProtectionDatabaseDriver();
                    ObjectProtection objProt = objProtDriver.loadObjectProtection(
                        ObjectProtection.buildPathSP(
                            storPoolName
                        ),
                        transMgr
                    );

                    StorPoolDefinitionDataDatabaseDriver storPoolDefDriver = DrbdManage.getStorPoolDefinitionDataDriver();
                    StorPoolDefinitionData storPoolDef = storPoolDefDriver.load(storPoolName, transMgr);

                    storPoolData = new StorPoolData(
                        UuidUtils.asUuid(resultSet.getBytes(NSP_UUID)),
                        objProt,
                        node,
                        storPoolDef,
                        null, // controller should not have an instance of storage driver.
                        resultSet.getString(NSP_DRIVER),
                        serGen,
                        transMgr
                    );
                    if (!cache(node, storPoolData))
                    {
                        storPoolData = cacheGet(node, storPoolDef);
                    }
                    else
                    {
                        // restore references
                    }
                }
                storPoolList.add(storPoolData);
            }
        }
        catch (InvalidNameException invalidNameExc)
        {
            resultSet.close();
            stmt.close();
            throw new DrbdSqlRuntimeException(
                String.format("Invalid storage name loaded from Table %s: %s ", TBL_NSP, resultSet.getString(NSP_POOL)),
                invalidNameExc
            );
        }

        resultSet.close();
        stmt.close();
        return storPoolList;
    }

    @Override
    public void delete(StorPoolData storPool, TransactionMgr transMgr) throws SQLException
    {
        try
        {
            Node node = storPool.getNode();
            StorPoolDefinition storPoolDfn;
            storPoolDfn = storPool.getDefinition(dbCtx);

            PreparedStatement stmt = transMgr.dbCon.prepareStatement(NSP_DELETE);

            stmt.setString(1, node.getName().value);
            stmt.setString(2, storPoolDfn.getName().value);

            stmt.executeUpdate();
            stmt.close();

            cacheRemove(node, storPoolDfn.getName());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            handleAccessDeniedException(accDeniedExc);
        }
    }

    @Override
    public void ensureEntryExists(StorPoolData storPoolData, TransactionMgr transMgr) throws SQLException
    {
        Node node = storPoolData.getNode();
        StorPoolDefinition storPoolDfn;
        try
        {
            storPoolDfn = storPoolData.getDefinition(dbCtx);
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            throw new ImplementationError(accessDeniedExc);
        }

        PreparedStatement stmt = transMgr.dbCon.prepareStatement(NSP_SELECT);
        stmt.setString(1, node.getName().value);
        stmt.setString(2, storPoolDfn.getName().value);
        ResultSet resultSet = stmt.executeQuery();

        StorPoolData cachedStorPool = cacheGet(node, storPoolData.getName());
        if (resultSet.next())
        {
            if (cachedStorPool == null)
            {
                cache(node, storPoolData);
            }
            else
            if (cachedStorPool != storPoolData)
            {
                resultSet.close();
                stmt.close();
                throw new DrbdSqlRuntimeException("A temporary StorPoolData instance is not allowed to override a persisted instance.");
            }
        }
        else
        {
            if (cachedStorPool != null)
            {
                // XXX: user deleted db entry during runtime - throw exception?
                // or just remove the item from the cache + detach item from parent (if needed) + warn the user?
            }
            create(storPoolData, transMgr);
        }

        resultSet.close();
        stmt.close();
    }

    private static void handleAccessDeniedException(AccessDeniedException accessDeniedExc) throws ImplementationError
    {
        throw new ImplementationError(
            "Database's access context has no permission to acces VolumeData's volNumber",
            accessDeniedExc
        );
    }

    private synchronized static boolean cache(Node node, StorPoolData storPoolData)
    {
        boolean ret = false;
        if (storPoolData != null)
        {
            PrimaryKey pk = getPk(node, storPoolData);
            boolean contains = storPoolCache.containsKey(pk);
            if (!contains)
            {
                storPoolCache.put(pk, storPoolData);
                ret = true;
            }
        }
        return ret;
    }

    private static StorPoolData cacheGet(Node node, StorPoolDefinition storPoolDfn)
    {
        return cacheGet(node, storPoolDfn.getName());
    }

    private static StorPoolData cacheGet(Node node, StorPoolName storPoolName)
    {
        return storPoolCache.get(getPk(node, storPoolName));
    }

    private synchronized static void cacheRemove(Node node, StorPoolName storPoolName)
    {
        storPoolCache.remove(getPk(node, storPoolName));
    }

    private static PrimaryKey getPk(Node node, StorPoolData storPoolData)
    {
        return new PrimaryKey(node.getName().value, storPoolData.getName().value);
    }

    private static PrimaryKey getPk(Node node, StorPoolName poolName)
    {
        return new PrimaryKey(node.getName().value, poolName.value);
    }

    /**
     * this method should only be called by tests or if you want a full-reload from the database
     */
    static synchronized void clearCache()
    {
        storPoolCache.clear();
    }
}
