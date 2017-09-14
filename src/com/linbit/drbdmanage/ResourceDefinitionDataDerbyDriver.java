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
import com.linbit.drbdmanage.propscon.SerialGenerator;
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
        try
        {
            PreparedStatement stmt = transMgr.dbCon.prepareStatement(RD_INSERT);
            stmt.setBytes(1, UuidUtils.asByteArray(resourceDefinition.getUuid()));
            stmt.setString(2, resourceDefinition.getName().value);
            stmt.setString(3, resourceDefinition.getName().displayValue);
            stmt.setLong(4, resourceDefinition.getFlags().getFlagsBits(dbCtx));
            stmt.executeUpdate();
            stmt.close();

            resDfnMap.put(resourceDefinition.getName(), resourceDefinition);
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            DerbyDriver.handleAccessDeniedException(accessDeniedExc);
        }
    }

    @Override
    public boolean exists(ResourceName resourceName, TransactionMgr transMgr) throws SQLException
    {
        PreparedStatement stmt = transMgr.dbCon.prepareStatement(RD_SELECT);
        stmt.setString(1, resourceName.value);
        ResultSet resultSet = stmt.executeQuery();

        boolean exists = resultSet.next();
        resultSet.close();
        stmt.close();
        return exists;
    }

    @Override
    public ResourceDefinitionData load(
        ResourceName resourceName,
        SerialGenerator serialGen,
        TransactionMgr transMgr
    )
        throws SQLException
    {
        PreparedStatement stmt = transMgr.dbCon.prepareStatement(RD_SELECT);
        stmt.setString(1, resourceName.value);
        ResultSet resultSet = stmt.executeQuery();

        ResourceDefinitionData resDfn = null;

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
                    resultSet.close();
                    stmt.close();
                    throw new ImplementationError(
                        "The display name of a valid ResourceName could not be restored",
                        invalidNameExc
                    );
                }


                ObjectProtectionDatabaseDriver objProtDriver = DrbdManage.getObjectProtectionDatabaseDriver();
                ObjectProtection objProt = objProtDriver.loadObjectProtection(
                    ObjectProtection.buildPath(resourceName),
                    transMgr
                );
                if (objProt != null)
                {
                    resDfn = new ResourceDefinitionData(
                        UuidUtils.asUuid(resultSet.getBytes(RD_UUID)),
                        objProt,
                        resourceName,
                        resultSet.getLong(RD_FLAGS),
                        serialGen,
                        transMgr
                    );
                    resDfnMap.put(resourceName, resDfn);

                    try
                    {
                        // restore connectionDefinitions
                        List<ConnectionDefinition> cons = connectionDefinitionDataDerbyDriver.loadAllConnectionsByResourceDefinition(
                            resDfn,
                            serialGen,
                            transMgr
                        );
                        for (ConnectionDefinition conDfn : cons)
                        {
                            int conDfnNr = conDfn.getConnectionNumber(dbCtx);
                            resDfn.addConnection(
                                dbCtx,
                                conDfn.getSourceNode(dbCtx).getName(),
                                conDfn.getTargetNode(dbCtx).getName(),
                                conDfnNr,
                                conDfn
                            );
                        }

                        // restore volumeDefinitions
                        List<VolumeDefinition> volDfns =
                            volumeDefinitionDataDerbyDriver.loadAllVolumeDefinitionsByResourceDefinition(
                            resDfn,
                            serialGen,
                            transMgr,
                            dbCtx
                        );
                        for (VolumeDefinition volDfn : volDfns)
                        {
                            resDfn.putVolumeDefinition(dbCtx, volDfn);
                        }

                        // restore resources
                        List<ResourceData> resList = resourceDataDerbyDriver.loadResourceDataByResourceDefinition(
                            resDfn,
                            serialGen,
                            transMgr,
                            dbCtx
                        );
                        for (ResourceData res : resList)
                        {
                            resDfn.addResource(dbCtx, res);
                        }
                    }
                    catch (AccessDeniedException accessDeniedExc)
                    {
                        resultSet.close();
                        stmt.close();
                        DerbyDriver.handleAccessDeniedException(accessDeniedExc);
                    }
                }
            }
        }
        resultSet.close();
        stmt.close();

        return resDfn;
    }

    @Override
    public void delete(ResourceDefinitionData resourceDefinition, TransactionMgr transMgr) throws SQLException
    {
        PreparedStatement stmt = transMgr.dbCon.prepareStatement(RD_DELETE);
        stmt.setString(1, resourceDefinition.getName().value);
        stmt.executeUpdate();

        stmt.close();
    }

    @Override
    public StateFlagsPersistence<ResourceDefinitionData> getStateFlagsPersistence()
    {
        return resDfnFlagPersistence;
    }

    private class ResDfnFlagsPersistence implements StateFlagsPersistence<ResourceDefinitionData>
    {
        @Override
        public void persist(ResourceDefinitionData resourceDefinition, long flags, TransactionMgr transMgr) throws SQLException
        {
            PreparedStatement stmt = transMgr.dbCon.prepareStatement(RD_UPDATE_FLAGS);
            stmt.setLong(1, flags);
            stmt.setString(2, resourceDefinition.getName().value);
            stmt.executeUpdate();

            stmt.close();
        }
    }
}
