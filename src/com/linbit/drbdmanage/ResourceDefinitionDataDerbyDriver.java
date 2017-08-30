package com.linbit.drbdmanage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Hashtable;
import java.util.List;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.dbdrivers.PrimaryKey;
import com.linbit.drbdmanage.dbdrivers.derby.DerbyConstants;
import com.linbit.drbdmanage.dbdrivers.interfaces.PropsConDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.ResourceDefinitionDataDatabaseDriver;
import com.linbit.drbdmanage.propscon.PropsConDerbyDriver;
import com.linbit.drbdmanage.propscon.PropsContainer;
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

    private AccessContext dbCtx;
    private ResourceName resName;
    private boolean resNameLoaded = false;

    private StateFlagsPersistence resDfnFlagPersistence;
    private PropsConDatabaseDriver propsDriver;


    private static Hashtable<PrimaryKey, ResourceDefinitionData> resDfnCache = new Hashtable<>();

    public ResourceDefinitionDataDerbyDriver(AccessContext accCtx, ResourceName resNameRef)
    {
        dbCtx = accCtx;
        resName = resNameRef;
        resDfnFlagPersistence = new ResDfnFlagsPersistence();
        propsDriver = new PropsConDerbyDriver(PropsContainer.buildPath(resNameRef));
    }

    @Override
    public void create(Connection con, ResourceDefinitionData resDfn) throws SQLException
    {
        if (!resDfn.getName().equals(resName))
        {
            throw new DrbdSqlRuntimeException(
                "Driver with fixed PrimaryKey received a not matching object to persist"
            );
        }

        try
        {
            PreparedStatement stmt = con.prepareStatement(RD_INSERT);
            stmt.setBytes(1, UuidUtils.asByteArray(resDfn.getUuid()));
            stmt.setString(2, resName.value);
            stmt.setString(3, resName.displayValue);
            stmt.setLong(4, resDfn.getFlags().getFlagsBits(dbCtx));
            stmt.executeUpdate();
            stmt.close();

            cache(resDfn);
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
    public boolean exists(Connection con) throws SQLException
    {
        PreparedStatement stmt = con.prepareStatement(RD_SELECT);
        stmt.setString(1, resName.value);
        ResultSet resultSet = stmt.executeQuery();

        boolean exists = resultSet.next();
        resultSet.close();
        stmt.close();
        return exists;
    }

    @Override
    public ResourceDefinitionData load(Connection con, SerialGenerator serialGen, TransactionMgr transMgr) throws SQLException
    {
        PreparedStatement stmt = con.prepareStatement(RD_SELECT);
        stmt.setString(1, resName.value);
        ResultSet resultSet = stmt.executeQuery();

        ResourceDefinitionData resDfn = cacheGet(resName);
        if (resDfn == null)
        {
            if (resultSet.next())
            {
                if (!resNameLoaded)
                {
                    // we could have been called with a half-valid resourceName.
                    // that means that someone stored "test"/"TEST" as dsp/name
                    // but another driver saw only the "TEST" reference.
                    // hence we will update the resourceName accordingly
                    try
                    {
                        resName = new ResourceName(resultSet.getString(RD_DSP_NAME));
                        resNameLoaded = true;
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
                }


                ObjectProtectionDatabaseDriver objProtDriver = DrbdManage.getObjectProtectionDatabaseDriver(
                    ObjectProtection.buildPath(resName)
                );
                ObjectProtection objProt = objProtDriver.loadObjectProtection(con);
                if (objProt != null)
                {
                    resDfn = new ResourceDefinitionData(
                        UuidUtils.asUuid(resultSet.getBytes(RD_UUID)),
                        objProt,
                        resName,
                        resultSet.getLong(RD_FLAGS),
                        serialGen,
                        transMgr
                    );
                    if (!cache(resDfn))
                    {
                        resDfn = cacheGet(resName);
                    }
                    else
                    {
                        try
                        {
                            // restore connectionDefinitions
                            List<ConnectionDefinition> cons =
                                ConnectionDefinitionDataDerbyDriver.loadAllConnectionsByResourceDefinition(
                                con,
                                resName,
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
                                con,
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
                                con,
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
    public void delete(Connection con) throws SQLException
    {
        PreparedStatement stmt = con.prepareStatement(RD_DELETE);
        stmt.setString(1, resName.value);
        stmt.executeUpdate();

        stmt.close();
        cacheRemove(resName);
    }

    @Override
    public StateFlagsPersistence getStateFlagsPersistence()
    {
        return resDfnFlagPersistence;
    }

    @Override
    public PropsConDatabaseDriver getPropsConDriver()
    {
        return propsDriver;
    }

    private synchronized static boolean cache(ResourceDefinitionData resDfn)
    {
        PrimaryKey pk = new PrimaryKey(resDfn.getName().value);
        boolean contains = resDfnCache.containsKey(pk);
        if (!contains)
        {
            resDfnCache.put(pk, resDfn);
        }
        return !contains;
    }

    private static ResourceDefinitionData cacheGet(ResourceName resName)
    {
        return resDfnCache.get(new PrimaryKey(resName.value));
    }

    private synchronized static void cacheRemove(ResourceName resName)
    {
        resDfnCache.remove(new PrimaryKey(resName.value));
    }

    /**
     * this method should only be called by tests or if you want a full-reload from the database
     */
    static synchronized void clearCache()
    {
        resDfnCache.clear();
    }

    private class ResDfnFlagsPersistence implements StateFlagsPersistence
    {

        @Override
        public void persist(Connection con, long flags) throws SQLException
        {
            PreparedStatement stmt = con.prepareStatement(RD_UPDATE_FLAGS);
            stmt.setLong(1, flags);
            stmt.setString(2, resName.value);
            stmt.executeUpdate();

            stmt.close();
        }
    }
}
