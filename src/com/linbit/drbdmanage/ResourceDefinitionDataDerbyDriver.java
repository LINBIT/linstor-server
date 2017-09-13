package com.linbit.drbdmanage;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Hashtable;
import java.util.List;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.core.DrbdManage;
import com.linbit.drbdmanage.dbdrivers.PrimaryKey;
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

    private static final Hashtable<PrimaryKey, ResourceDefinitionData> RES_DFN_CACHE = new Hashtable<>();

    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;

    private final StateFlagsPersistence<ResourceDefinitionData> resDfnFlagPersistence;

    public ResourceDefinitionDataDerbyDriver(AccessContext accCtx, ErrorReporter errorReporterRef)
    {
        dbCtx = accCtx;
        errorReporter = errorReporterRef;
        resDfnFlagPersistence = new ResDfnFlagsPersistence();
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

            cache(resourceDefinition);
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            throw new ImplementationError(
                "Database's access context has no permission to get storPoolDefinition",
                accessDeniedExc
            );
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

        ResourceDefinitionData resDfn = cacheGet(resourceName);
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
                    if (!cache(resDfn))
                    {
                        resDfn = cacheGet(resourceName);
                    }
                    else
                    {
                        try
                        {
                            // restore connectionDefinitions
                            List<ConnectionDefinition> cons =
                                ConnectionDefinitionDataDerbyDriver.loadAllConnectionsByResourceDefinition(
                                resourceName,
                                serialGen,
                                transMgr,
                                dbCtx
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
                                VolumeDefinitionDataDerbyDriver.loadAllVolumeDefinitionsByResourceDefinition(
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
                            List<ResourceData> resList = ResourceDataDerbyDriver.loadResourceDataByResourceDefinition(
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
                            throw new ImplementationError(
                                "Database's access context has no permission to get storPoolDefinition",
                                accessDeniedExc
                            );
                        }
                    }
                }
            }
        }
        else
        {
            if (!resultSet.next())
            {
                // XXX: user deleted db entry during runtime - throw exception?
                // or just remove the item from the cache + detach item from parent (if needed) + warn the user?
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
        cacheRemove(resourceDefinition.getName());
    }

    @Override
    public StateFlagsPersistence<ResourceDefinitionData> getStateFlagsPersistence()
    {
        return resDfnFlagPersistence;
    }

    private synchronized static boolean cache(ResourceDefinitionData resDfn)
    {
        PrimaryKey pk = new PrimaryKey(resDfn.getName().value);
        boolean contains = RES_DFN_CACHE.containsKey(pk);
        if (!contains)
        {
            RES_DFN_CACHE.put(pk, resDfn);
        }
        return !contains;
    }

    private static ResourceDefinitionData cacheGet(ResourceName resName)
    {
        return RES_DFN_CACHE.get(new PrimaryKey(resName.value));
    }

    private synchronized static void cacheRemove(ResourceName resName)
    {
        RES_DFN_CACHE.remove(new PrimaryKey(resName.value));
    }

    /**
     * this method should only be called by tests or if you want a full-reload from the database
     */
    static synchronized void clearCache()
    {
        RES_DFN_CACHE.clear();
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
