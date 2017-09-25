package com.linbit.drbdmanage;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.TransactionMgr;
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
    private static final String RD_FLAGS = DerbyConstants.RESOURCE_FLAGS;

    private static final String RD_SELECT =
        " SELECT " + RD_UUID + ", " + RD_NAME + ", " + RD_DSP_NAME + ", " + RD_FLAGS +
        " FROM " + TBL_RES_DEF +
        " WHERE " + RD_NAME + " = ?";
    private static final String RD_INSERT =
        " INSERT INTO " + TBL_RES_DEF +
        " VALUES (?, ?, ?, ?)";
    private static final String RD_UPDATE_FLAGS =
        " UPDATE " + TBL_RES_DEF +
        " SET " + RD_FLAGS + " = ? " +
        " WHERE " + RD_NAME + " = ?";
    private static final String RD_DELETE =
        " DELETE FROM " + TBL_RES_DEF +
        " WHERE " + RD_NAME + " = ?";

    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;
    private final Map<ResourceName, ResourceDefinition> resDfnMap;

    private final StateFlagsPersistence<ResourceDefinitionData> resDfnFlagPersistence;

    private final ConnectionDefinitionDataDerbyDriver connectionDefinitionDataDerbyDriver;
    private final ResourceDataDerbyDriver resourceDataDerbyDriver;
    private final VolumeDefinitionDataDerbyDriver volumeDefinitionDataDerbyDriver;

    public ResourceDefinitionDataDerbyDriver(
        AccessContext accCtx,
        ErrorReporter errorReporterRef,
        Map<ResourceName, ResourceDefinition> resDfnMapRef,
        ConnectionDefinitionDataDerbyDriver connectionDefinitionDriver,
        ResourceDataDerbyDriver resourceDriver,
        VolumeDefinitionDataDerbyDriver volumeDefinitionDriver
    )
    {
        dbCtx = accCtx;
        errorReporter = errorReporterRef;
        resDfnFlagPersistence = new ResDfnFlagsPersistence();
        resDfnMap = resDfnMapRef;

        connectionDefinitionDataDerbyDriver = connectionDefinitionDriver;
        resourceDataDerbyDriver = resourceDriver;
        volumeDefinitionDataDerbyDriver = volumeDefinitionDriver;
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
            stmt.setLong(4, resourceDefinition.getFlags().getFlagsBits(dbCtx));
            stmt.executeUpdate();

            resDfnMap.put(resourceDefinition.getName(), resourceDefinition);

            errorReporter.logDebug("ResourceDefinition created %s", getDebugId(resourceDefinition));
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
                // resourceDefinition loads connectionDefinitions loads resourceDefinitions...
                // to break this cycle, we check if we are already in this cylce
                resDfn = (ResourceDefinitionData) resDfnMap.get(resourceName);
                if (resDfn == null)
                {
                    if (resultSet.next())
                    {
                        try
                        {
                            resourceName = new ResourceName(resultSet.getString(RD_DSP_NAME));
                        }
                        catch (InvalidNameException invalidNameExc)
                        {
                            throw new ImplementationError(
                                "The display name of a valid ResourceName could not be restored",
                                invalidNameExc
                            );
                        }
                        ObjectProtection objProt = getObjectProtection(resourceName, transMgr);

                        resDfn = new ResourceDefinitionData(
                            UuidUtils.asUuid(resultSet.getBytes(RD_UUID)),
                            objProt,
                            resourceName,
                            resultSet.getLong(RD_FLAGS),
                            transMgr
                        );
                        // cache the resDfn BEFORE we load the conDfns
                        resDfnMap.put(resourceName, resDfn);

                        errorReporter.logTrace("ResourceDefinition instance created %s", getTraceId(resDfn));

                        // restore connectionDefinitions
                        List<ConnectionDefinition> cons = connectionDefinitionDataDerbyDriver.loadAllConnectionsByResourceDefinition(
                            resDfn,
                            transMgr
                        );
                        for (ConnectionDefinition conDfn : cons)
                        {
                            resDfn.addConnection(
                                dbCtx,
                                conDfn.getSourceNode(dbCtx).getName(),
                                conDfn.getTargetNode(dbCtx).getName(),
                                conDfn.getConnectionNumber(dbCtx),
                                conDfn
                            );
                        }
                        errorReporter.logTrace(
                            "Restored ResourceDefinition's ConnectionDefinitions %s",
                            getTraceId(resDfn)
                        );

                        // restore volumeDefinitions
                        List<VolumeDefinition> volDfns =
                            volumeDefinitionDataDerbyDriver.loadAllVolumeDefinitionsByResourceDefinition(
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
                        List<ResourceData> resList = resourceDataDerbyDriver.loadResourceDataByResourceDefinition(
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
                    else
                    {
                        errorReporter.logWarning("ResourceDefinition not found in the DB %s", getDebugId(resourceName));
                    }
                }
                else
                {
                    errorReporter.logDebug("ResourceDefinition loaded from cache %s", getDebugId(resDfn));
                }
            }
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            DerbyDriver.handleAccessDeniedException(accessDeniedExc);
        }
        return resDfn;
    }

    private ObjectProtection getObjectProtection(ResourceName resourceName, TransactionMgr transMgr)
        throws SQLException, ImplementationError
    {
        ObjectProtectionDatabaseDriver objProtDriver = DrbdManage.getObjectProtectionDatabaseDriver();
        ObjectProtection objProt = objProtDriver.loadObjectProtection(
            ObjectProtection.buildPath(resourceName),
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
        errorReporter.logDebug("ResourceDfinition deleted %s", getDebugId(resourceDefinition));
    }

    @Override
    public StateFlagsPersistence<ResourceDefinitionData> getStateFlagsPersistence()
    {
        return resDfnFlagPersistence;
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
                errorReporter.logDebug(
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
}
