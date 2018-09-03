package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.StorPool.InitMaps;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.dbdrivers.GenericDbDriver;
import com.linbit.linstor.dbdrivers.derby.DbConstants;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDataDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionDatabaseDriver;
import com.linbit.linstor.storage.StorageDriverLoader;
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
import java.util.HashMap;
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
    private static final String SP_FSM_NAME = DbConstants.FREE_SPACE_MGR_NAME;
    private static final String SP_FSM_DSP_NAME = DbConstants.FREE_SPACE_MGR_DSP_NAME;

    private static final String SELECT_ALL =
        " SElECT " + SP_UUID + ", " + SP_NODE + ", " + SP_POOL + ", " + SP_DRIVER + ", " +
                     SP_FSM_NAME + ", " + SP_FSM_DSP_NAME +
        " FROM " + TBL_NSP;
    private static final String SP_SELECT_BY_NODE =
        SELECT_ALL +
        " WHERE " + SP_NODE + " = ?";
    private static final String SP_SELECT_BY_NODE_AND_SP =
        SP_SELECT_BY_NODE +
        " AND "  + SP_POOL + " = ?";
    private static final String SELECT_ALL_FSM =
        " SELECT DISTINCT " + SP_FSM_DSP_NAME +
        " FROM " + TBL_NSP;

    private static final String SP_INSERT =
        " INSERT INTO " + TBL_NSP +
        " (" + SP_UUID + ", " + SP_NODE + ", " + SP_POOL + ", " + SP_DRIVER + ", " +
               SP_FSM_NAME + ", " + SP_FSM_DSP_NAME + ")" +
        " VALUES (?, ?, ?, ?, ?, ?)";
    private static final String SP_DELETE =
        " DELETE FROM " + TBL_NSP +
        " WHERE " + SP_NODE + " = ? AND " +
        "       " + SP_POOL + " = ?";

    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;

    private final ObjectProtectionDatabaseDriver objProtDriver;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public StorPoolDataGenericDbDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        ObjectProtectionDatabaseDriver objProtDriverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        dbCtx = dbCtxRef;
        errorReporter = errorReporterRef;
        objProtDriver = objProtDriverRef;
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
            FreeSpaceMgrName fsmName = storPoolData.getFreeSpaceTracker().getName();
            stmt.setString(1, storPoolData.getUuid().toString());
            stmt.setString(2, storPoolData.getNode().getName().value);
            stmt.setString(3, storPoolData.getName().value);
            stmt.setString(4, storPoolData.getDriverName());
            stmt.setString(5, fsmName.value);
            stmt.setString(6, fsmName.displayValue);
            stmt.executeUpdate();
        }
        errorReporter.logTrace("StorPool created %s", getId(storPoolData));
    }

    public Map<FreeSpaceMgrName, FreeSpaceMgr> loadAllFreeSpaceMgrs()
        throws SQLException
    {
        Map<FreeSpaceMgrName, FreeSpaceMgr> ret = new HashMap<>();
        errorReporter.logTrace("Loading all free space managers");
        try (PreparedStatement stmt = getConnection().prepareStatement(SELECT_ALL_FSM))
        {
            try (ResultSet resultSet = stmt.executeQuery())
            {
                while (resultSet.next())
                {
                    try
                    {
                        FreeSpaceMgrName fsmName = FreeSpaceMgrName.restoreName(resultSet.getString(SP_FSM_DSP_NAME));
                        ret.put(
                            fsmName,
                            new FreeSpaceMgr(
                                dbCtx,
                                getObjectProtection(fsmName),
                                fsmName,
                                transMgrProvider,
                                transObjFactory
                            )
                        );
                    }
                    catch (InvalidNameException invalidNameExc)
                    {
                        throw new LinStorSqlRuntimeException(
                            "The stored free space manager name '" + resultSet.getString(SP_FSM_DSP_NAME) + "' " +
                                "could not be restored",
                            invalidNameExc
                        );
                    }
                }
            }
        }
        return ret;
    }

    public Map<StorPoolData, InitMaps> loadAll(
        Map<NodeName, ? extends Node> nodesMap,
        Map<StorPoolName, ? extends StorPoolDefinition> storPoolDfnMap,
        Map<FreeSpaceMgrName, FreeSpaceMgr> freeSpaceMgrMap
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
                        FreeSpaceMgrName fsmName = FreeSpaceMgrName.restoreName(resultSet.getString(SP_FSM_DSP_NAME));

                        Pair<StorPoolData, InitMaps> pair = restoreStorPool(
                            resultSet,
                            nodesMap.get(nodeName),
                            storPoolDfnMap.get(storPoolName),
                            freeSpaceMgrMap.get(fsmName)
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
        StorPoolDefinition storPoolDfn,
        FreeSpaceMgr freeSpaceMgr
    )
        throws SQLException
    {
        Map<String, Volume> vlmMap = new TreeMap<>();
        StorPoolData storPool = new StorPoolData(
            java.util.UUID.fromString(resultSet.getString(SP_UUID)),
            node,
            storPoolDfn,
            StorageDriverLoader.getKind(resultSet.getString(SP_DRIVER)),
            freeSpaceMgr,
            false,
            this,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider,
            vlmMap
        );
        return new Pair<>(storPool, new StorPoolInitMaps(vlmMap));
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

    private String getId(String nodeName, String poolName)
    {
        return "(NodeName=" + nodeName + " PoolName=" + poolName + ")";
    }

    private ObjectProtection getObjectProtection(FreeSpaceMgrName fsmName) throws SQLException
    {
        ObjectProtection objProt = objProtDriver.loadObjectProtection(
            ObjectProtection.buildPath(fsmName),
            false // no need to log a warning, as we would fail then anyways
        );
        if (objProt == null)
        {
            throw new ImplementationError(
                "FreeSpaceManager is missing an entry in ObjProt table! " + getId(fsmName),
                null
            );
        }
        return objProt;
    }

    private String getId(FreeSpaceMgrName fsmName)
    {
        return "(FreeSpaceMgrName=" + fsmName.displayValue + ")";
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
