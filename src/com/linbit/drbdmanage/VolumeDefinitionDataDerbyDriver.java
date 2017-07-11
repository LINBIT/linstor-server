package com.linbit.drbdmanage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;

import com.linbit.ImplementationError;
import com.linbit.ObjectDatabaseDriver;
import com.linbit.TransactionMgr;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.drbdmanage.VolumeDefinition.VlmDfnFlags;
import com.linbit.drbdmanage.dbdrivers.PrimaryKey;
import com.linbit.drbdmanage.dbdrivers.derby.DerbyConstants;
import com.linbit.drbdmanage.dbdrivers.interfaces.PropsConDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.VolumeDefinitionDataDatabaseDriver;
import com.linbit.drbdmanage.propscon.PropsConDerbyDriver;
import com.linbit.drbdmanage.propscon.PropsContainer;
import com.linbit.drbdmanage.propscon.SerialGenerator;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;
import com.linbit.utils.UuidUtils;

public class VolumeDefinitionDataDerbyDriver implements VolumeDefinitionDataDatabaseDriver
{
    private static final String TBL_VOL_DFN = DerbyConstants.TBL_VOLUME_DEFINITIONS;

    private static final String VD_UUID = DerbyConstants.UUID;
    private static final String VD_RES_NAME = DerbyConstants.RESOURCE_NAME;
    private static final String VD_ID = DerbyConstants.VLM_NR;
    private static final String VD_SIZE = DerbyConstants.VLM_SIZE;
    private static final String VD_MINOR_NR = DerbyConstants.VLM_MINOR_NR;
    private static final String VD_FLAGS = DerbyConstants.VLM_FLAGS;

    private static final String VD_SELECT =
        " SELECT " + VD_UUID + ", " + VD_SIZE + ", " + VD_MINOR_NR + ", " + VD_FLAGS +
        " FROM " + TBL_VOL_DFN +
        " WHERE " + VD_RES_NAME + " = ? AND " +
                    VD_ID       + " = ?";
    private static final String VD_SELECT_BY_RES_DFN = 
        " SELECT " +  VD_UUID + ", " + VD_RES_NAME + ", " + VD_ID + ", " + 
                      VD_SIZE + ", " + VD_MINOR_NR + ", " + VD_FLAGS + 
        " FROM " + TBL_VOL_DFN +
        " WHERE " + VD_RES_NAME  + " = ?";
    private static final String VD_INSERT =
        " INSERT INTO " + TBL_VOL_DFN +
        " VALUES (?, ?, ?, ?, ?, ?)";
    private static final String VD_DELETE =
        " DELETE FROM " + TBL_VOL_DFN +
        " WHERE " + VD_RES_NAME + " = ? AND " +
                    VD_ID       + " = ?";

    private AccessContext dbCtx;
    private ResourceDefinition resDfn;
    private VolumeNumber volNr;

    private PropsConDatabaseDriver propsDriver;

    private static Hashtable<PrimaryKey, VolumeDefinitionData> volDfnCache = new Hashtable<>();

    public VolumeDefinitionDataDerbyDriver(AccessContext accCtx, ResourceDefinition resDfnRef, VolumeNumber volNrRef)
    {
        dbCtx = accCtx;
        resDfn = resDfnRef;
        volNr = volNrRef;
        propsDriver = new PropsConDerbyDriver(PropsContainer.buildPath(resDfnRef.getName(), volNrRef));
    }

    @Override
    public void create(Connection con, VolumeDefinitionData volDfnData) throws SQLException
    {
        try(PreparedStatement stmt = con.prepareStatement(VD_INSERT))
        {
            stmt.setBytes(1, UuidUtils.asByteArray(volDfnData.getUuid()));
            stmt.setString(2, volDfnData.getResourceDfn().getName().value);
            stmt.setInt(3, volDfnData.getVolumeNumber(dbCtx).value);
            stmt.setLong(4, volDfnData.getVolumeSize(dbCtx));
            stmt.setInt(5, volDfnData.getMinorNr(dbCtx).value);
            stmt.setLong(6, volDfnData.getFlags().getFlagsBits(dbCtx));

            stmt.executeUpdate();

            cache(volDfnData, dbCtx);
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            throw new ImplementationError(
                "Database's access context has no permission to access VolumeDefinition",
                accessDeniedExc
            );
        }
    }

    @Override
    public VolumeDefinitionData load(Connection con, TransactionMgr transMgr, SerialGenerator serialGen) throws SQLException
    {
        PreparedStatement stmt = con.prepareStatement(VD_SELECT);
        stmt.setString(1, resDfn.getName().value);
        stmt.setInt(2, volNr.value);
        ResultSet resultSet = stmt.executeQuery();

        VolumeDefinitionData ret = cacheGet(resDfn, volNr);

        if (ret == null)
        {
            if (resultSet.next())
            {
                ret = restoreVolumeDefinition(resultSet, resDfn, volNr, serialGen, transMgr, dbCtx);
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

    private static VolumeDefinitionData restoreVolumeDefinition(
        ResultSet resultSet, 
        ResourceDefinition resDfn,
        VolumeNumber volNr,
        SerialGenerator serialGen, 
        TransactionMgr transMgr,
        AccessContext accCtx
    ) 
        throws SQLException
    { 
        VolumeDefinitionData ret = null;
        try
        {
            long lFlags = resultSet.getLong(VD_FLAGS);
            Set<VlmDfnFlags> flags = new HashSet<>();

            for (VlmDfnFlags flag : VlmDfnFlags.values())
            {
                if ((lFlags & flag.flagValue) == flag.flagValue)
                {
                    flags.add(flag);
                }
            }

            ret = new VolumeDefinitionData(
                UuidUtils.asUUID(resultSet.getBytes(VD_UUID)),
                accCtx, // volumeDefinition does not have objProt, but require access to their resource's objProt
                resDfn,
                volNr,
                new MinorNumber(resultSet.getInt(VD_MINOR_NR)),
                resultSet.getLong(VD_SIZE),
                transMgr,
                serialGen,
                flags
            );
            if (!cache(ret, accCtx))
            {
                ret = cacheGet(resDfn, volNr);
            }
            else
            {
                // restore references
            }
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            resultSet.close();
            throw new ImplementationError(
                "Database's access context has no permission to create VolumeDefinition",
                accessDeniedExc
            );
        }
        catch (MdException mdExc)
        {
            resultSet.close();
            throw new DrbdSqlRuntimeException(
                "Invalid volume size: " + resultSet.getLong(VD_SIZE),
                mdExc
            );
        }
        catch (ValueOutOfRangeException valueOutOfRangeExc)
        {
            resultSet.close();
            throw new DrbdSqlRuntimeException(
                "Invalid minor number: " + resultSet.getInt(VD_MINOR_NR),
                valueOutOfRangeExc
            );
        }
        return ret;
    }
    

    public static List<VolumeDefinition> loadAllVolumeDefinitionsByResourceDefinition(
            Connection con, 
            ResourceDefinition resDfn, 
            SerialGenerator serialGen, 
            TransactionMgr transMgr, 
            AccessContext accCtx
    )
        throws SQLException
    {
        PreparedStatement stmt = con.prepareStatement(VD_SELECT_BY_RES_DFN);
        stmt.setString(1, resDfn.getName().value);
        ResultSet resultSet = stmt.executeQuery();
        List<VolumeDefinition> ret = new ArrayList<>();
        while (resultSet.next())
        {
            VolumeNumber volNr;
            try
            {
                volNr = new VolumeNumber(resultSet.getInt(VD_ID));
            }
            catch (ValueOutOfRangeException valueOutOfRangeExc)
            {
                resultSet.close();
                stmt.close();
                throw new DrbdSqlRuntimeException(
                    "Invalid volume number: " + resultSet.getInt(VD_ID),
                    valueOutOfRangeExc
                );
            }
            
            VolumeDefinition volDfn = restoreVolumeDefinition(
                resultSet, 
                resDfn, 
                volNr,
                serialGen, 
                transMgr, 
                accCtx
            );
            
            ret.add(volDfn);
        }
        resultSet.close();
        stmt.close();
        return ret;
    }        

    @Override
    public void delete(Connection con) throws SQLException
    {
        PreparedStatement stmt = con.prepareStatement(VD_DELETE);

        stmt.setString(1, resDfn.getName().value);
        stmt.setInt(2, volNr.value);
        stmt.executeUpdate();
        stmt.close();

        cacheRemove(resDfn, volNr);
    }

    @Override
    public StateFlagsPersistence getStateFlagsPersistence()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ObjectDatabaseDriver<MinorNumber> getMinorNumberDriver()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ObjectDatabaseDriver<Long> getVolumeSizeDriver()
    {
        // TODO Auto-generated method stub
        return null;
    }

    private synchronized static boolean cache(VolumeDefinitionData vdd, AccessContext dbCtx) throws AccessDeniedException
    {
        PrimaryKey pk = new PrimaryKey(vdd.getResourceDfn().getName().value, vdd.getVolumeNumber(dbCtx).value);
        boolean contains = volDfnCache.containsKey(pk);
        if (!contains)
        {
            volDfnCache.put(pk, vdd);
        }
        return !contains;
    }

    private static VolumeDefinitionData cacheGet(ResourceDefinition resDfn, VolumeNumber volNr)
    {
        return volDfnCache.get(new PrimaryKey(resDfn.getName().value, volNr.value));
    }

    private synchronized static void cacheRemove(ResourceDefinition resDfn, VolumeNumber volNr)
    {
        volDfnCache.remove(new PrimaryKey(resDfn.getName().value, volNr.value));
    }

    /**
     * this method should only be called by tests or if you want a full-reload from the database
     */
    static synchronized void clearCache()
    {
        volDfnCache.clear();
    }

    @Override
    public PropsConDatabaseDriver getPropsDriver()
    {
        return propsDriver;
    }
}
