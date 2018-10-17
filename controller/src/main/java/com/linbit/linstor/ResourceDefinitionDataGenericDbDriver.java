package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.ResourceDefinition.InitMaps;
import com.linbit.linstor.ResourceDefinition.RscDfnFlags;
import com.linbit.linstor.ResourceDefinition.TransportType;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.dbdrivers.GenericDbDriver;
import com.linbit.linstor.dbdrivers.derby.DbConstants;
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
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.TreeMap;

@Singleton
public class ResourceDefinitionDataGenericDbDriver implements ResourceDefinitionDataDatabaseDriver
{
    private static final String TBL_RES_DEF = DbConstants.TBL_RESOURCE_DEFINITIONS;

    private static final String RD_UUID = DbConstants.UUID;
    private static final String RD_NAME = DbConstants.RESOURCE_NAME;
    private static final String RD_DSP_NAME = DbConstants.RESOURCE_DSP_NAME;
    private static final String RD_PORT = DbConstants.TCP_PORT;
    private static final String RD_SECRET = DbConstants.SECRET;
    private static final String RD_TRANS_TYPE = DbConstants.TRANSPORT_TYPE;
    private static final String RD_FLAGS = DbConstants.RESOURCE_FLAGS;

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

    private final StateFlagsPersistence<ResourceDefinitionData> resDfnFlagPersistence;
    private final SingleColumnDatabaseDriver<ResourceDefinitionData, TcpPortNumber> portDriver;
    private final SingleColumnDatabaseDriver<ResourceDefinitionData, TransportType> transTypeDriver;

    private final ObjectProtectionDatabaseDriver objProtDriver;
    private final PropsContainerFactory propsContainerFactory;
    private final DynamicNumberPool tcpPortPool;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public ResourceDefinitionDataGenericDbDriver(
        @SystemContext AccessContext accCtx,
        ErrorReporter errorReporterRef,
        ObjectProtectionDatabaseDriver objProtDriverRef,
        PropsContainerFactory propsContainerFactoryRef,
        @Named(NumberPoolModule.TCP_PORT_POOL) DynamicNumberPool tcpPortPoolRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        dbCtx = accCtx;
        errorReporter = errorReporterRef;
        objProtDriver = objProtDriverRef;
        propsContainerFactory = propsContainerFactoryRef;
        tcpPortPool = tcpPortPoolRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
        resDfnFlagPersistence = new ResDfnFlagsPersistence();
        portDriver = new PortDriver();
        transTypeDriver = new TransportTypeDriver();
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
            GenericDbDriver.handleAccessDeniedException(accessDeniedExc);
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

    public Map<ResourceDefinitionData, InitMaps> loadAll() throws SQLException
    {
        errorReporter.logTrace("Loading all ResourceDefinitions");
        Map<ResourceDefinitionData, InitMaps> rscDfnMap = new TreeMap<>();
        try (PreparedStatement stmt = getConnection().prepareStatement(RD_SELECT_ALL))
        {
            try (ResultSet resultSet = stmt.executeQuery())
            {
                while (resultSet.next())
                {
                    Pair<ResourceDefinitionData, InitMaps> pair = restoreRscDfn(resultSet);
                    rscDfnMap.put(pair.objA, pair.objB);
                }
            }
        }
        errorReporter.logTrace("Loaded %d ResourceDefinitions", rscDfnMap.size());
        return rscDfnMap;
    }

    private Pair<ResourceDefinitionData, InitMaps> restoreRscDfn(ResultSet resultSet) throws SQLException
    {
        Pair<ResourceDefinitionData, InitMaps> retPair = new Pair<>();
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

        ObjectProtection objProt = getObjectProtection(resourceName);

        Map<VolumeNumber, VolumeDefinition> vlmDfnMap = new TreeMap<>();
        Map<Pair<NodeName, ResourceType>, Resource> rscMap = new TreeMap<>();
        Map<SnapshotName, SnapshotDefinition> snapshotDfnMap = new TreeMap<>();

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
            transMgrProvider,
            vlmDfnMap,
            rscMap,
            snapshotDfnMap,
            null // TODO: restore layer data
        );

        retPair.objA = resDfn;
        retPair.objB = new RscDfnInitMaps(vlmDfnMap, rscMap, snapshotDfnMap);

        errorReporter.logTrace("ResourceDefinition instance created %s", getId(resDfn));
        return retPair;
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
                GenericDbDriver.handleAccessDeniedException(accDeniedExc);
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
                    "Updating ResourceDefinition's port from [%d] to [%d] %s",
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
                    "ResourceDefinition's port updated from [%d] to [%d] %s",
                    resourceDefinition.getPort(dbCtx).value,
                    port.value,
                    getId(resourceDefinition)
                );
            }
            catch (AccessDeniedException accDeniedExc)
            {
                GenericDbDriver.handleAccessDeniedException(accDeniedExc);
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
                GenericDbDriver.handleAccessDeniedException(accDeniedExc);
            }
        }
    }

    private class RscDfnInitMaps implements ResourceDefinition.InitMaps
    {
        private final Map<VolumeNumber, VolumeDefinition> vlmDfnMap;
        private final Map<Pair<NodeName, ResourceType>, Resource> rscMap;
        private final Map<SnapshotName, SnapshotDefinition> snapshotDfnMap;

        RscDfnInitMaps(
            Map<VolumeNumber, VolumeDefinition> vlmDfnMapRef,
            Map<Pair<NodeName, ResourceType>, Resource> rscMapRef,
            Map<SnapshotName, SnapshotDefinition> snapshotDfnMapRef
        )
        {
            this.vlmDfnMap = vlmDfnMapRef;
            this.rscMap = rscMapRef;
            this.snapshotDfnMap = snapshotDfnMapRef;
        }

        @Override
        public Map<Pair<NodeName, ResourceType>, Resource> getRscMap()
        {
            return rscMap;
        }

        @Override
        public Map<VolumeNumber, VolumeDefinition> getVlmDfnMap()
        {
            return vlmDfnMap;
        }

        @Override
        public Map<SnapshotName, SnapshotDefinition> getSnapshotDfnMap()
        {
            return snapshotDfnMap;
        }
    }
}
