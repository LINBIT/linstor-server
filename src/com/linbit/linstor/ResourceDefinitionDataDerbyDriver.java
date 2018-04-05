package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.ResourceDefinition.RscDfnFlags;
import com.linbit.linstor.ResourceDefinition.TransportType;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.annotation.Uninitialized;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.dbdrivers.DerbyDriver;
import com.linbit.linstor.dbdrivers.derby.DerbyConstants;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDefinitionDataDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionDatabaseDriver;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.utils.StringUtils;

import javax.inject.Inject;
import javax.inject.Named;
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

@Singleton
public class ResourceDefinitionDataDerbyDriver implements ResourceDefinitionDataDatabaseDriver
{
    private static final String TBL_RES_DEF = DerbyConstants.TBL_RESOURCE_DEFINITIONS;

    private static final String RD_UUID = DerbyConstants.UUID;
    private static final String RD_NAME = DerbyConstants.RESOURCE_NAME;
    private static final String RD_DSP_NAME = DerbyConstants.RESOURCE_DSP_NAME;
    private static final String RD_PORT = DerbyConstants.TCP_PORT;
    private static final String RD_SECRET = DerbyConstants.SECRET;
    private static final String RD_TRANS_TYPE = DerbyConstants.TRANSPORT_TYPE;
    private static final String RD_FLAGS = DerbyConstants.RESOURCE_FLAGS;

    private static final String RD_SELECT =
        " SELECT " + RD_UUID + ", " + RD_NAME + ", " + RD_DSP_NAME + ", " +
                     RD_SECRET + ", " + RD_FLAGS + ", " + RD_PORT + ", " + RD_TRANS_TYPE +
        " FROM " + TBL_RES_DEF +
        " WHERE " + RD_NAME + " = ?";
    private static final String RD_SELECT_ALL =
        " SELECT " + RD_UUID + ", " + RD_NAME + ", " + RD_DSP_NAME + ", " +
                     RD_SECRET + ", " + RD_FLAGS + ", " + RD_PORT + ", " + RD_TRANS_TYPE +
        " FROM " + TBL_RES_DEF;
    private static final String RD_INSERT =
        " INSERT INTO " + TBL_RES_DEF +
        " (" + RD_UUID + ", " + RD_NAME + ", " + RD_DSP_NAME + ", " +
               RD_PORT + ", " + RD_FLAGS + ", " + RD_SECRET + ", " + RD_TRANS_TYPE +
        " )" +
        " VALUES (?, ?, ?, ?, ?, ?, ?)";
    private static final String RD_UPDATE_FLAGS =
        " UPDATE " + TBL_RES_DEF +
        " SET " + RD_FLAGS + " = ? " +
        " WHERE " + RD_NAME + " = ?";
    private static final String RD_UPDATE_PORT =
        " UPDATE " + TBL_RES_DEF +
        " SET " + RD_PORT + " = ? " +
        " WHERE " + RD_NAME + " = ?";
    private static final String RD_UPDATE_TRANS_TYPE =
        " UPDATE " + TBL_RES_DEF +
        " SET " + RD_TRANS_TYPE + " = ? " +
        " WHERE " + RD_NAME + " = ?";
    private static final String RD_DELETE =
        " DELETE FROM " + TBL_RES_DEF +
        " WHERE " + RD_NAME + " = ?";

    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;
    private final CoreModule.ResourceDefinitionMap resDfnMap;

    private final Map<ResourceName, ResourceDefinitionData> rscDfnCache;
    private boolean cacheCleared = false;

    private final StateFlagsPersistence<ResourceDefinitionData> resDfnFlagPersistence;
    private final SingleColumnDatabaseDriver<ResourceDefinitionData, TcpPortNumber> portDriver;
    private final SingleColumnDatabaseDriver<ResourceDefinitionData, TransportType> transTypeDriver;

    private final Provider<ResourceDataDerbyDriver> resourceDriverProvider;
    private final Provider<VolumeDefinitionDataDerbyDriver> volumeDefinitionDriverProvider;
    private final ObjectProtectionDatabaseDriver objProtDriver;
    private final PropsContainerFactory propsContainerFactory;
    private final DynamicNumberPool tcpPortPool;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public ResourceDefinitionDataDerbyDriver(
        @SystemContext AccessContext accCtx,
        ErrorReporter errorReporterRef,
        @Uninitialized CoreModule.ResourceDefinitionMap resDfnMapRef,
        Provider<ResourceDataDerbyDriver> resourceDriverProviderRef,
        Provider<VolumeDefinitionDataDerbyDriver> volumeDefinitionDriverProviderRef,
        ObjectProtectionDatabaseDriver objProtDriverRef,
        PropsContainerFactory propsContainerFactoryRef,
        @Named(NumberPoolModule.UNINITIALIZED_TCP_PORT_POOL) DynamicNumberPool tcpPortPoolRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        dbCtx = accCtx;
        errorReporter = errorReporterRef;
        resourceDriverProvider = resourceDriverProviderRef;
        volumeDefinitionDriverProvider = volumeDefinitionDriverProviderRef;
        objProtDriver = objProtDriverRef;
        propsContainerFactory = propsContainerFactoryRef;
        tcpPortPool = tcpPortPoolRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
        resDfnFlagPersistence = new ResDfnFlagsPersistence();
        portDriver = new PortDriver();
        transTypeDriver = new TransportTypeDriver();
        resDfnMap = resDfnMapRef;
        rscDfnCache = new HashMap<>();
    }

    @Override
    @SuppressWarnings("checkstyle:magicnumber")
    public void create(ResourceDefinitionData resourceDefinition) throws SQLException
    {
        errorReporter.logTrace("Creating ResourceDfinition %s", getId(resourceDefinition));
        try (PreparedStatement stmt = getConnection().prepareStatement(RD_INSERT))
        {
            stmt.setString(1, resourceDefinition.getUuid().toString());
            stmt.setString(2, resourceDefinition.getName().value);
            stmt.setString(3, resourceDefinition.getName().displayValue);
            stmt.setInt(4, resourceDefinition.getPort(dbCtx).value);
            stmt.setLong(5, resourceDefinition.getFlags().getFlagsBits(dbCtx));
            stmt.setString(6, resourceDefinition.getSecret(dbCtx));
            stmt.setString(7, resourceDefinition.getTransportType(dbCtx).name());
            stmt.executeUpdate();

            errorReporter.logTrace("ResourceDefinition created %s", getId(resourceDefinition));
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            DerbyDriver.handleAccessDeniedException(accessDeniedExc);
        }
    }

    @Override
    public boolean exists(ResourceName resourceName) throws SQLException
    {
        boolean exists = false;
        try (PreparedStatement stmt = getConnection().prepareStatement(RD_SELECT))
        {
            stmt.setString(1, resourceName.value);
            try (ResultSet resultSet = stmt.executeQuery())
            {
                exists = resultSet.next();
            }
        }
        return exists;
    }

    @Override
    public ResourceDefinitionData load(ResourceName resourceName, boolean logWarnIfNotExists)
        throws SQLException
    {
        errorReporter.logTrace("Loading ResourceDefinition %s", getId(resourceName));
        ResourceDefinitionData resDfn = null;
        try (PreparedStatement stmt = getConnection().prepareStatement(RD_SELECT))
        {
            stmt.setString(1, resourceName.value);
            try (ResultSet resultSet = stmt.executeQuery())
            {
                if (resultSet.next())
                {
                    resDfn = load(resultSet);
                }
                else
                if (logWarnIfNotExists)
                {
                    errorReporter.logWarning("ResourceDefinition not found in the DB %s", getId(resourceName));
                }
            }
        }
        return resDfn;
    }

    public List<ResourceDefinitionData> loadAll() throws SQLException
    {
        errorReporter.logTrace("Loading all ResourceDefinitions");
        List<ResourceDefinitionData> list = new ArrayList<>();
        try (PreparedStatement stmt = getConnection().prepareStatement(RD_SELECT_ALL))
        {
            try (ResultSet resultSet = stmt.executeQuery())
            {
                while (resultSet.next())
                {
                    list.add(
                        load(resultSet)
                    );
                }
            }
        }
        errorReporter.logTrace("Loaded %d ResourceDefinitions", list.size());
        return list;
    }

    private ResourceDefinitionData load(ResultSet resultSet) throws SQLException
    {
        ResourceDefinitionData resDfn;
        ResourceName resourceName;
        TcpPortNumber port;
        try
        {
            resourceName = new ResourceName(resultSet.getString(RD_DSP_NAME));
            port = new TcpPortNumber(resultSet.getInt(RD_PORT));
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw new LinStorSqlRuntimeException(
                String.format(
                    "The display name of a stored ResourceDefinition in the table %s could not be restored. " +
                        "(invalid display ResName=%s)",
                    TBL_RES_DEF,
                    resultSet.getString(RD_DSP_NAME)
                ),
                invalidNameExc
            );
        }
        catch (ValueOutOfRangeException valOutOfRangeExc)
        {
            throw new LinStorSqlRuntimeException(
                String.format(
                    "The port number of a stored ResourceDefinition in the table %s could not be restored. " +
                        "(ResName=%s, invalid port=%d)",
                        TBL_RES_DEF,
                        resultSet.getString(RD_DSP_NAME),
                        resultSet.getInt(RD_PORT)
                    ),
                valOutOfRangeExc
            );
        }

        // resourceDefinition loads resources loads resourceDefinitions...
        // to break this cycle, we check if we are already in this cycle
        resDfn = (ResourceDefinitionData) resDfnMap.get(resourceName);
        if (resDfn == null)
        {
            resDfn = rscDfnCache.get(resourceName);
        }
        if (resDfn == null)
        {
            try
            {
                ObjectProtection objProt = getObjectProtection(resourceName);

                resDfn = new ResourceDefinitionData(
                    java.util.UUID.fromString(resultSet.getString(RD_UUID)),
                    objProt,
                    resourceName,
                    port,
                    tcpPortPool,
                    resultSet.getLong(RD_FLAGS),
                    resultSet.getString(RD_SECRET),
                    TransportType.byValue(resultSet.getString(RD_TRANS_TYPE)),
                    this,
                    propsContainerFactory,
                    transObjFactory,
                    transMgrProvider
                );
                // cache the resDfn BEFORE we load the conDfns
                if (!cacheCleared)
                {
                    rscDfnCache.put(resourceName, resDfn);
                }

                errorReporter.logTrace("ResourceDefinition instance created %s", getId(resDfn));

                // restore volumeDefinitions
                List<VolumeDefinition> volDfns =
                    volumeDefinitionDriverProvider.get().loadAllVolumeDefinitionsByResourceDefinition(
                    resDfn,
                    dbCtx
                );
                for (VolumeDefinition volDfn : volDfns)
                {
                    resDfn.putVolumeDefinition(dbCtx, volDfn);
                }
                errorReporter.logTrace(
                    "Restored ResourceDefinition's VolumeDefinitions %s",
                    getId(resDfn)
                );

                // restore resources
                List<ResourceData> resList = resourceDriverProvider.get().loadResourceDataByResourceDefinition(
                    resDfn,
                    dbCtx
                );
                for (ResourceData res : resList)
                {
                    resDfn.addResource(dbCtx, res);
                }

                errorReporter.logTrace("Restored ResourceDefinition's Resources %s", getId(resDfn));
            }
            catch (AccessDeniedException accessDeniedExc)
            {
                DerbyDriver.handleAccessDeniedException(accessDeniedExc);
            }

        }
        else
        {
            errorReporter.logTrace("ResourceDefinition loaded from cache %s", getId(resDfn));
        }
        return resDfn;
    }

    private ObjectProtection getObjectProtection(ResourceName resourceName)
        throws SQLException, ImplementationError
    {
        ObjectProtection objProt = objProtDriver.loadObjectProtection(
            ObjectProtection.buildPath(resourceName),
            false // no need to log a warning, as we would fail then anyways
        );
        if (objProt == null)
        {
            throw new ImplementationError(
                "ResourceDefinition's DB entry exists, but is missing an entry in ObjProt table! " +
                getId(resourceName), null
            );
        }
        return objProt;
    }

    public void clearCache()
    {
        cacheCleared = true;
        rscDfnCache.clear();
    }

    @Override
    public void delete(ResourceDefinitionData resourceDefinition) throws SQLException
    {
        errorReporter.logTrace("Deleting ResourceDefinition %s", getId(resourceDefinition));
        try (PreparedStatement stmt = getConnection().prepareStatement(RD_DELETE))
        {
            stmt.setString(1, resourceDefinition.getName().value);
            stmt.executeUpdate();
        }
        errorReporter.logTrace("ResourceDfinition deleted %s", getId(resourceDefinition));
    }

    @Override
    public StateFlagsPersistence<ResourceDefinitionData> getStateFlagsPersistence()
    {
        return resDfnFlagPersistence;
    }

    @Override
    public SingleColumnDatabaseDriver<ResourceDefinitionData, TcpPortNumber> getPortDriver()
    {
        return portDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<ResourceDefinitionData, TransportType> getTransportTypeDriver()
    {
        return transTypeDriver;
    }

    private Connection getConnection()
    {
        return transMgrProvider.get().getConnection();
    }

    private String getId(ResourceDefinitionData resourceDefinition)
    {
        return getId(resourceDefinition.getName().displayValue);
    }

    private String getId(ResourceName resourceName)
    {
        return getId(resourceName.displayValue);
    }

    private String getId(String resName)
    {
        return "(ResName=" + resName + ")";
    }

    private class ResDfnFlagsPersistence implements StateFlagsPersistence<ResourceDefinitionData>
    {
        @Override
        public void persist(ResourceDefinitionData resourceDefinition, long flags)
            throws SQLException
        {
            try
            {
                String fromFlags = StringUtils.join(
                    FlagsHelper.toStringList(
                        RscDfnFlags.class,
                        resourceDefinition.getFlags().getFlagsBits(dbCtx)
                    ),
                    ", "
                );
                String toFlags = StringUtils.join(
                    FlagsHelper.toStringList(
                        RscDfnFlags.class,
                        flags
                    ),
                    ", "
                );
                errorReporter.logTrace(
                    "Updating ResourceDefinition's flags from [%s] to [%s] %s",
                    fromFlags,
                    toFlags,
                    getId(resourceDefinition)
                );
                try (PreparedStatement stmt = getConnection().prepareStatement(RD_UPDATE_FLAGS))
                {
                    stmt.setLong(1, flags);
                    stmt.setString(2, resourceDefinition.getName().value);
                    stmt.executeUpdate();
                }
                errorReporter.logTrace(
                    "ResourceDefinition's flags updated from [%s] to [%s] %s",
                    fromFlags,
                    toFlags,
                    getId(resourceDefinition)
                );
            }
            catch (AccessDeniedException accDeniedExc)
            {
                DerbyDriver.handleAccessDeniedException(accDeniedExc);
            }
        }
    }

    private class PortDriver implements SingleColumnDatabaseDriver<ResourceDefinitionData, TcpPortNumber>
    {
        @Override
        public void update(ResourceDefinitionData resourceDefinition, TcpPortNumber port)
            throws SQLException
        {
            try
            {
                errorReporter.logTrace(
                    "Updating ResourceDefinition's flags from [%d] to [%d] %s",
                    resourceDefinition.getPort(dbCtx).value,
                    port.value,
                    getId(resourceDefinition)
                );
                try (PreparedStatement stmt = getConnection().prepareStatement(RD_UPDATE_PORT))
                {
                    stmt.setInt(1, port.value);
                    stmt.setString(2, resourceDefinition.getName().value);
                    stmt.executeUpdate();
                }
                errorReporter.logTrace(
                    "ResourceDefinition's flags updated from [%d] to [%d] %s",
                    resourceDefinition.getPort(dbCtx).value,
                    port.value,
                    getId(resourceDefinition)
                );
            }
            catch (AccessDeniedException accDeniedExc)
            {
                DerbyDriver.handleAccessDeniedException(accDeniedExc);
            }
        }
    }

    private class TransportTypeDriver implements SingleColumnDatabaseDriver<ResourceDefinitionData, TransportType>
    {

        @Override
        public void update(
            ResourceDefinitionData resourceDefinition,
            TransportType transType
        )
            throws SQLException
        {
            try
            {
                errorReporter.logTrace(
                    "Updating ResourceDefinition's transport type from [%s] to [%s] %s",
                    resourceDefinition.getTransportType(dbCtx).name(),
                    transType.name(),
                    getId(resourceDefinition)
                );
                try (
                    PreparedStatement stmt =
                        getConnection().prepareStatement(RD_UPDATE_TRANS_TYPE)
                )
                {
                    stmt.setString(1, transType.name());
                    stmt.setString(2, resourceDefinition.getName().value);
                    stmt.executeUpdate();
                }
                errorReporter.logTrace(
                    "ResourceDefinition's transport type updated from [%s] to [%s] %s",
                    resourceDefinition.getTransportType(dbCtx).name(),
                    transType.name(),
                    getId(resourceDefinition)
                );
            }
            catch (AccessDeniedException accDeniedExc)
            {
                DerbyDriver.handleAccessDeniedException(accDeniedExc);
            }
        }

    }
}
