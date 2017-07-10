package com.linbit.drbdmanage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Hashtable;

import com.linbit.ImplementationError;
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
        " SELECT " + RD_UUID + ", " + RD_NAME + ", " + RD_DSP_NAME +
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

        ResourceDefinitionData ret = cacheGet(resName);
        if (ret == null)
        {
            if (resultSet.next())
            {
                ObjectProtectionDatabaseDriver objProtDriver = DrbdManage.getObjectProtectionDatabaseDriver(
                    ObjectProtection.buildPath(resName)
                );
                ObjectProtection objProt = objProtDriver.loadObjectProtection(con);
                if (objProt != null)
                {
                    ret = new ResourceDefinitionData(
                        objProt,
                        UuidUtils.asUUID(resultSet.getBytes(RD_UUID)),
                        resName,
                        serialGen,
                        transMgr
                    );
                }
            }
        }
        else
        {
            if (!resultSet.next())
            {
                // XXX: user deleted db entry during runtime - throw exception?
                // or just remove the item from the cache + node.removeRes(cachedRes) + warn the user?
            }
        }
        resultSet.close();
        stmt.close();

        return ret;
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

    private static void cache(ResourceDefinitionData resDfn)
    {
        resDfnCache.put(new PrimaryKey(resDfn.getName().value), resDfn);
    }

    private static ResourceDefinitionData cacheGet(ResourceName resName)
    {
        return resDfnCache.get(new PrimaryKey(resName.value));
    }

    private static void cacheRemove(ResourceName resName)
    {
        resDfnCache.remove(new PrimaryKey(resName.value));
    }

    /**
     * this method should only be called by tests or if you want a full-reload from the database
     */
    static void clearCache()
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
