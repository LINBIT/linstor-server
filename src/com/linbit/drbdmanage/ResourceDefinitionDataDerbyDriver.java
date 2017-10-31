package com.linbit.drbdmanage;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.TransactionMgr;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbdmanage.core.DrbdManage;
import com.linbit.drbdmanage.dbdrivers.DerbyDriver;
import com.linbit.drbdmanage.dbdrivers.derby.DerbyConstants;
import com.linbit.drbdmanage.dbdrivers.interfaces.ResourceDefinitionDataDatabaseDriver;
import com.linbit.drbdmanage.logging.ErrorReporter;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.ObjectProtection;
import com.linbit.drbdmanage.security.ObjectProtectionDatabaseDriver;
import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;
import com.linbit.utils.UuidUtils;

public class ResourceDefinitionDataDerbyDriver implements ResourceDefinitionDataDatabaseDriver
{
    private static final String TBL_RES_DEF = DerbyConstants.TBL_RESOURCE_DEFINITIONS;

    private static final String RD_UUID = DerbyConstants.UUID;
    private static final String RD_NAME = DerbyConstants.RESOURCE_NAME;
    private static final String RD_DSP_NAME = DerbyConstants.RESOURCE_DSP_NAME;
    private static final String RD_PORT = DerbyConstants.TCP_PORT;
    private static final String RD_FLAGS = DerbyConstants.RESOURCE_FLAGS;

    private static final String RD_SELECT =
        " SELECT " + RD_UUID + ", " + RD_NAME + ", " + RD_DSP_NAME + ", " +
                     RD_FLAGS + ", " + RD_PORT +
        " FROM " + TBL_RES_DEF +
        " WHERE " + RD_NAME + " = ?";
    private static final String RD_SELECT_ALL =
        " SELECT " + RD_UUID + ", " + RD_NAME + ", " + RD_DSP_NAME + ", " +
                     RD_FLAGS + ", " + RD_PORT +
        " FROM " + TBL_RES_DEF;
    private static final String RD_INSERT =
        " INSERT INTO " + TBL_RES_DEF +
        " VALUES (?, ?, ?, ?, ?)";
    private static final String RD_UPDATE_FLAGS =
        " UPDATE " + TBL_RES_DEF +
        " SET " + RD_FLAGS + " = ? " +
        " WHERE " + RD_NAME + " = ?";
    private static final String RD_UPDATE_PORT =
        " UPDATE " + TBL_RES_DEF +
        " SET " + RD_PORT + " = ? " +
        " WHERE " + RD_NAME + " = ?";
    private static final String RD_DELETE =
        " DELETE FROM " + TBL_RES_DEF +
        " WHERE " + RD_NAME + " = ?";

    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;
    private final Map<ResourceName, ResourceDefinition> resDfnMap;

    private final StateFlagsPersistence<ResourceDefinitionData> resDfnFlagPersistence;
    private final SingleColumnDatabaseDriver<ResourceDefinitionData, TcpPortNumber> portDriver;

    private ResourceDataDerbyDriver resourceDriver;
    private VolumeDefinitionDataDerbyDriver volumeDefinitionDriver;

    public ResourceDefinitionDataDerbyDriver(
        AccessContext accCtx,
        ErrorReporter errorReporterRef,
        Map<ResourceName, ResourceDefinition> resDfnMapRef
    )
    {
        dbCtx = accCtx;
        errorReporter = errorReporterRef;
        resDfnFlagPersistence = new ResDfnFlagsPersistence();
        portDriver = new PortDriver();
        resDfnMap = resDfnMapRef;
    }

    public void initialize(
        ResourceDataDerbyDriver resourceDriverRef,
        VolumeDefinitionDataDerbyDriver volumeDefinitionDriverRef
    )
    {
        resourceDriver = resourceDriverRef;
        volumeDefinitionDriver = volumeDefinitionDriverRef;
    }


    @Override
    public void create(ResourceDefinitionData resourceDefinition, TransactionMgr transMgr) throws SQLException
    {
        errorReporter.logTrace("Creating ResourceDfinition %s", getTraceId(resourceDefinition));
        try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(RD_INSERT))
        {
            stmt.setBytes(1, UuidUtils.asByteArray(resourceDefinition.getUuid()));
            stmt.setString(2, resourceDefinition.getName().value);
            stmt.setString(3, resourceDefinition.getName().displayValue);
            stmt.setInt(4, resourceDefinition.getPort(dbCtx).value);
            stmt.setLong(5, resourceDefinition.getFlags().getFlagsBits(dbCtx));
            stmt.executeUpdate();

            resDfnMap.put(resourceDefinition.getName(), resourceDefinition);

            errorReporter.logTrace("ResourceDefinition created %s", getDebugId(resourceDefinition));
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            DerbyDriver.handleAccessDeniedException(accessDeniedExc);
        }
    }

    @Override
    public boolean exists(ResourceName resourceName, TransactionMgr transMgr) throws SQLException
    {
        boolean exists = false;
        try(PreparedStatement stmt = transMgr.dbCon.prepareStatement(RD_SELECT))
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
    public ResourceDefinitionData load(
        ResourceName resourceName,
        boolean logWarnIfNotExists,
        TransactionMgr transMgr
    )
        throws SQLException
    {
        errorReporter.logTrace("Loading ResourceDefinition %s", getTraceId(resourceName));
        ResourceDefinitionData resDfn = null;
        try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(RD_SELECT))
        {
            stmt.setString(1, resourceName.value);
            try (ResultSet resultSet = stmt.executeQuery())
            {
                if (resultSet.next())
                {
                    resDfn = load(resultSet, transMgr);
                }
                else
                if (logWarnIfNotExists)
                {
                    errorReporter.logWarning("ResourceDefinition not found in the DB %s", getDebugId(resourceName));
                }
            }
        }
        return resDfn;
    }

    public void loadAll(TransactionMgr transMgr) throws SQLException
    {
        errorReporter.logTrace("Loading all ResourceDefinitions");
        try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(RD_SELECT_ALL))
        {
            try (ResultSet resultSet = stmt.executeQuery())
            {
                while (resultSet.next())
                {
                    load(resultSet, transMgr); // we do not care about the return value
                    // the loaded resDfn(s) get cached anyways, and thus the controller gets
                    // the references that way
                }
            }
        }
        errorReporter.logTrace("Loaded %d ResourceDefinitions", resDfnMap.size());
    }

    private ResourceDefinitionData load(ResultSet resultSet, TransactionMgr transMgr) throws SQLException
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
            throw new DrbdSqlRuntimeException(
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
            throw new DrbdSqlRuntimeException(
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
            try
            {
                ObjectProtection objProt = getObjectProtection(resourceName, transMgr);

                resDfn = new ResourceDefinitionData(
                    UuidUtils.asUuid(resultSet.getBytes(RD_UUID)),
                    objProt,
                    resourceName,
                    port,
                    resultSet.getLong(RD_FLAGS),
                    transMgr
                );
                // cache the resDfn BEFORE we load the conDfns
                resDfnMap.put(resourceName, resDfn);

                errorReporter.logTrace("ResourceDefinition instance created %s", getTraceId(resDfn));

                // restore volumeDefinitions
                List<VolumeDefinition> volDfns =
                    volumeDefinitionDriver.loadAllVolumeDefinitionsByResourceDefinition(
                    resDfn,
                    transMgr,
                    dbCtx
                );
                for (VolumeDefinition volDfn : volDfns)
                {
                    resDfn.putVolumeDefinition(dbCtx, volDfn);
                }
                errorReporter.logTrace(
                    "Restored ResourceDefinition's VolumeDefinitions %s",
                    getTraceId(resDfn)
                );

                // restore resources
                List<ResourceData> resList = resourceDriver.loadResourceDataByResourceDefinition(
                    resDfn,
                    transMgr,
                    dbCtx
                );
                for (ResourceData res : resList)
                {
                    resDfn.addResource(dbCtx, res);
                }

                errorReporter.logTrace("Restored ResourceDefinition's Resources %s", getTraceId(resDfn));
            }
            catch (AccessDeniedException accessDeniedExc)
            {
                DerbyDriver.handleAccessDeniedException(accessDeniedExc);
            }

        }
        else
        {
            errorReporter.logTrace("ResourceDefinition loaded from cache %s", getDebugId(resDfn));
        }
        return resDfn;
    }

    private ObjectProtection getObjectProtection(ResourceName resourceName, TransactionMgr transMgr)
        throws SQLException, ImplementationError
    {
        ObjectProtectionDatabaseDriver objProtDriver = DrbdManage.getObjectProtectionDatabaseDriver();
        ObjectProtection objProt = objProtDriver.loadObjectProtection(
            ObjectProtection.buildPath(resourceName),
            false, // no need to log a warning, as we would fail then anyways
            transMgr
        );
        if (objProt == null)
        {
            throw new ImplementationError(
                "ResourceDefinition's DB entry exists, but is missing an entry in ObjProt table! " + getTraceId(resourceName),
                null
            );
        }
        return objProt;
    }

    @Override
    public void delete(ResourceDefinitionData resourceDefinition, TransactionMgr transMgr) throws SQLException
    {
        errorReporter.logTrace("Deleting ResourceDefinition %s", getTraceId(resourceDefinition));
        try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(RD_DELETE))
        {
            stmt.setString(1, resourceDefinition.getName().value);
            stmt.executeUpdate();
        }
        errorReporter.logTrace("ResourceDfinition deleted %s", getDebugId(resourceDefinition));
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

    private String getTraceId(ResourceDefinitionData resourceDefinition)
    {
        return getId(resourceDefinition.getName().value);
    }

    private String getTraceId(ResourceName resourceName)
    {
        return getId(resourceName.value);
    }

    private String getDebugId(ResourceDefinitionData resourceDefinition)
    {
        return getId(resourceDefinition.getName().displayValue);
    }

    private String getDebugId(ResourceName resourceName)
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
        public void persist(ResourceDefinitionData resourceDefinition, long flags, TransactionMgr transMgr) throws SQLException
        {
            try
            {
                errorReporter.logTrace(
                    "Updating ResourceDefinition's flags from [%s] to [%s] %s",
                    Long.toBinaryString(resourceDefinition.getFlags().getFlagsBits(dbCtx)),
                    Long.toBinaryString(flags),
                    getTraceId(resourceDefinition)
                );
                try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(RD_UPDATE_FLAGS))
                {
                    stmt.setLong(1, flags);
                    stmt.setString(2, resourceDefinition.getName().value);
                    stmt.executeUpdate();
                }
                errorReporter.logTrace(
                    "ResourceDefinition's flags updated from [%s] to [%s] %s",
                    Long.toBinaryString(resourceDefinition.getFlags().getFlagsBits(dbCtx)),
                    Long.toBinaryString(flags),
                    getDebugId(resourceDefinition)
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
        public void update(ResourceDefinitionData resourceDefinition, TcpPortNumber port, TransactionMgr transMgr)
            throws SQLException
        {
            try
            {
                errorReporter.logTrace(
                    "Updating ResourceDefinition's flags from [%d] to [%d] %s",
                    resourceDefinition.getPort(dbCtx).value,
                    port.value,
                    getTraceId(resourceDefinition)
                );
                try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(RD_UPDATE_PORT))
                {
                    stmt.setInt(1, port.value);
                    stmt.setString(2, resourceDefinition.getName().value);
                    stmt.executeUpdate();
                }
                errorReporter.logTrace(
                    "ResourceDefinition's flags updated from [%d] to [%d] %s",
                    resourceDefinition.getPort(dbCtx).value,
                    port.value,
                    getDebugId(resourceDefinition)
                );
            }
            catch (AccessDeniedException accDeniedExc)
            {
                DerbyDriver.handleAccessDeniedException(accDeniedExc);
            }
        }
    }
}
