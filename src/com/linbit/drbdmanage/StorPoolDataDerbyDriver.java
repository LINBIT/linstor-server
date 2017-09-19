package com.linbit.drbdmanage;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.core.DrbdManage;
import com.linbit.drbdmanage.dbdrivers.DerbyDriver;
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

    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;

    public StorPoolDataDerbyDriver(AccessContext dbCtxRef, ErrorReporter errorReporterRef)
    {
        dbCtx = dbCtxRef;
        errorReporter = errorReporterRef;
    }

    @Override
    public void create(StorPoolData storPoolData, TransactionMgr transMgr) throws SQLException
    {
        errorReporter.logTrace("Creating StorPool %s", getTraceId(storPoolData));
        try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(NSP_INSERT))
        {
            stmt.setBytes(1, UuidUtils.asByteArray(storPoolData.getUuid()));
            stmt.setString(2, storPoolData.getNode().getName().value);
            stmt.setString(3, storPoolData.getName().value);
            stmt.setString(4, storPoolData.getDriverName());
            stmt.executeUpdate();
        }
        errorReporter.logDebug("StorPool created %s", getDebugId(storPoolData));
    }

    @Override
    public StorPoolData load(
        Node node,
        StorPoolDefinition storPoolDfn,
        SerialGenerator serialGen,
        TransactionMgr transMgr
    )
        throws SQLException
    {
        errorReporter.logTrace("Loading StorPool %s", getTraceId(node, storPoolDfn));
        StorPoolData sp = null;
        try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(NSP_SELECT))
        {
            stmt.setString(1, node.getName().value);
            stmt.setString(2, storPoolDfn.getName().value);
            try (ResultSet resultSet = stmt.executeQuery())
            {
                sp = cacheGet(node, storPoolDfn);
                if (sp == null)
                {
                    if (resultSet.next())
                    {
                        ObjectProtection objProt = loadObjectProtection(
                            (NodeData) node,
                            storPoolDfn.getName(),
                            transMgr
                        );

                        sp = new StorPoolData(
                            UuidUtils.asUuid(resultSet.getBytes(NSP_UUID)),
                            objProt,
                            node,
                            storPoolDfn,
                            null,   // storageDriver, has to be null in the controller
                            resultSet.getString(NSP_DRIVER),
                            serialGen,
                            transMgr
                        );
                        errorReporter.logDebug("StorPool loaded from DB", getDebugId(sp));
                    }
                    else
                    {
                        errorReporter.logWarning(
                            "StorPool was not found in the DB %s",
                            getDebugId(node, storPoolDfn)
                        );
                    }
                }
                else
                {
                    errorReporter.logDebug("StorPool loaded from cache %s", getDebugId(sp));
                }
            }
        }
        return sp;
    }

    public List<StorPoolData> loadStorPools(
        NodeData node,
        SerialGenerator serGen,
        TransactionMgr transMgr
    )
        throws SQLException
    {
        errorReporter.logTrace("Loading all StorPools from Node (NodeName=%s)", node.getName().value);
        List<StorPoolData> storPoolList = new ArrayList<>();
        try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(NSP_SELECT_BY_NODE))
        {
            stmt.setString(1, node.getName().value);

            try (ResultSet resultSet = stmt.executeQuery())
            {
                while(resultSet.next())
                {
                    StorPoolName storPoolName;
                    try
                    {
                        storPoolName = new StorPoolName(resultSet.getString(NSP_POOL));
                    }
                    catch (InvalidNameException invalidNameExc)
                    {
                        throw new DrbdSqlRuntimeException(
                            String.format(
                                "Invalid StorName loaded from Table %s: %s ",
                                TBL_NSP,
                                resultSet.getString(NSP_POOL)
                            ),
                            invalidNameExc
                        );
                    }
                    StorPoolData storPoolData = cacheGet(node, storPoolName);
                    if (storPoolData == null)
                    {
                        ObjectProtection objProt = loadObjectProtection(node, storPoolName, transMgr);

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
                        errorReporter.logDebug("Loaded StorPool from DB %s", getDebugId(storPoolData));
                    }
                    else
                    {
                        errorReporter.logDebug("Loaded StorPool from cache %s", getDebugId(storPoolData));
                    }
                    storPoolList.add(storPoolData);
                }
            }
        }
        return storPoolList;
    }

    private ObjectProtection loadObjectProtection(
        NodeData node,
        StorPoolName storPoolName,
        TransactionMgr transMgr
    )
        throws SQLException, ImplementationError
    {
        ObjectProtectionDatabaseDriver objProtDriver = DrbdManage.getObjectProtectionDatabaseDriver();
        ObjectProtection objProt = objProtDriver.loadObjectProtection(
            ObjectProtection.buildPathSP(storPoolName),
            transMgr
        );
        if (objProt == null)
        {
            throw new ImplementationError(
                String.format(
                    "StorPool's DB entry exists, but the corresponding entry in ObjProt table is missing! %s",
                    getId(
                        node.getName().value,
                        storPoolName.value
                    )
                ),
                null
            );
        }
        return objProt;
    }

    @Override
    public void delete(StorPoolData storPool, TransactionMgr transMgr) throws SQLException
    {
        errorReporter.logTrace("Deleting StorPool %s", getTraceId(storPool));
        try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(NSP_DELETE))
        {
            Node node = storPool.getNode();
            StorPoolDefinition storPoolDfn = storPool.getDefinition(dbCtx);

            stmt.setString(1, node.getName().value);
            stmt.setString(2, storPoolDfn.getName().value);

            stmt.executeUpdate();
            errorReporter.logDebug("StorPool deleted %s", getDebugId(storPool));
        }
        catch (AccessDeniedException accDeniedExc)
        {
            DerbyDriver.handleAccessDeniedException(accDeniedExc);
        }
    }

    @Override
    public void ensureEntryExists(StorPoolData storPoolData, TransactionMgr transMgr) throws SQLException
    {
        errorReporter.logTrace("Ensuring StorPool exists %s", getTraceId(storPoolData));
        Node node = storPoolData.getNode();
        StorPoolDefinition storPoolDfn;
        try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(NSP_SELECT);)
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
                    errorReporter.logTrace("StorPool existed, nothing to do %s", getTraceId(storPoolData));
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
        StorPoolData ret = null;
        try
        {
            ret = (StorPoolData) node.getStorPool(dbCtx, storPoolName);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            DerbyDriver.handleAccessDeniedException(accDeniedExc);
        }
        return ret;
    }

    private String getTraceId(StorPoolData storPoolData)
    {
        return getId(
            storPoolData.getNode().getName().value,
            storPoolData.getName().value
        );
    }

    private String getTraceId(Node node, StorPoolDefinition storPoolDfn)
    {
        return getId(
            node.getName().value,
            storPoolDfn.getName().value
        );
    }

    private String getDebugId(StorPoolData storPoolData)
    {
        return getId(
            storPoolData.getNode().getName().displayValue,
            storPoolData.getName().displayValue
        );
    }

    private String getDebugId(Node node, StorPoolDefinition storPoolDfn)
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
}
