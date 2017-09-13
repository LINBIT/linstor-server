package com.linbit.drbdmanage;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import com.linbit.ImplementationError;
import com.linbit.TransactionMgr;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbdmanage.Volume.VlmFlags;
import com.linbit.drbdmanage.core.DrbdManage;
import com.linbit.drbdmanage.dbdrivers.PrimaryKey;
import com.linbit.drbdmanage.dbdrivers.derby.DerbyConstants;
import com.linbit.drbdmanage.dbdrivers.interfaces.VolumeDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.VolumeDefinitionDataDatabaseDriver;
import com.linbit.drbdmanage.logging.ErrorReporter;
import com.linbit.drbdmanage.propscon.SerialGenerator;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.stateflags.StateFlags;
import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;
import com.linbit.utils.UuidUtils;

public class VolumeDataDerbyDriver implements VolumeDataDatabaseDriver
{
    private static final String TBL_VOL = DerbyConstants.TBL_VOLUMES;

    private static final String VOL_UUID = DerbyConstants.UUID;
    private static final String VOL_NODE_NAME = DerbyConstants.NODE_NAME;
    private static final String VOL_RES_NAME = DerbyConstants.RESOURCE_NAME;
    private static final String VOL_ID = DerbyConstants.VLM_NR;
    private static final String VOL_BLOCK_DEVICE = DerbyConstants.BLOCK_DEVICE_PATH;
    private static final String VOL_META_DISK = DerbyConstants.META_DISK_PATH;
    private static final String VOL_FLAGS = DerbyConstants.VLM_FLAGS;

    private static final String VOL_SELECT_BY_RES =
        " SELECT " +  VOL_UUID + ", " + VOL_ID + ", " + VOL_BLOCK_DEVICE + ", " +
                    VOL_META_DISK + ", " + VOL_FLAGS +
        " FROM " + TBL_VOL +
        " WHERE " + VOL_NODE_NAME + " = ? AND " +
                    VOL_RES_NAME  + " = ?";
    private static final String VOL_SELECT =  VOL_SELECT_BY_RES +
        " AND " +   VOL_ID +        " = ?";
    private static final String VOL_INSERT =
        " INSERT INTO " + TBL_VOL +
        " VALUES (?, ?, ?, ?, ?, ?, ?)";
    private static final String VOL_UPDATE_FLAGS =
        " UPDATE " + TBL_VOL +
        " SET " + VOL_FLAGS + " = ? " +
        " WHERE " + VOL_NODE_NAME + " = ? AND " +
                    VOL_RES_NAME  + " = ? AND " +
                    VOL_ID        + " = ?";
    private static final String VOL_DELETE =
        " DELETE FROM " + TBL_VOL +
        " WHERE " + VOL_NODE_NAME + " = ? AND " +
                    VOL_RES_NAME  + " = ? AND " +
                    VOL_ID        + " = ?";

    private static Hashtable<PrimaryKey, VolumeData> volCache = new Hashtable<>();

    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;

    private final StateFlagsPersistence<VolumeData> flagPersistenceDriver;

    public VolumeDataDerbyDriver(AccessContext privCtx, ErrorReporter errorReporterRef)
    {
        dbCtx = privCtx;
        errorReporter = errorReporterRef;
        flagPersistenceDriver = new VolFlagsPersistence();
    }

    @Override
    public VolumeData load(
        Resource resource,
        VolumeDefinition volumeDefintion,
        SerialGenerator serialGen,
        TransactionMgr transMgr
    )
        throws SQLException
    {
        VolumeData ret = null;
        try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(VOL_SELECT))
        {
            stmt.setString(1, resource.getAssignedNode().getName().value);
            stmt.setString(2, resource.getDefinition().getName().value);
            stmt.setInt(3, volumeDefintion.getVolumeNumber(dbCtx).value);
            ResultSet resultSet = stmt.executeQuery();

            List<VolumeData> volList = load(
                dbCtx,
                resource,
                resultSet,
                serialGen,
                transMgr
            );
            resultSet.close();

            ret = cacheGet(
                resource.getAssignedNode(),
                resource.getDefinition(),
                volumeDefintion.getVolumeNumber(dbCtx)
            );

            if (ret == null)
            {
                if (!volList.isEmpty())
                {
                    ret = volList.get(0);
                }
            }
            else
            {
                if (volList.isEmpty())
                {
                    // XXX: user deleted db entry during runtime - throw exception?
                    // or just remove the item from the cache + detach item from parent (if needed) + warn the user?
                }
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            handleAccessDeniedException(accDeniedExc);
        }
        return ret;
    }

    public static List<VolumeData> loadAllVolumesByResource(
        Resource resRef,
        TransactionMgr transMgr,
        SerialGenerator serialGen,
        AccessContext accCtx
    )
        throws SQLException
    {
        PreparedStatement stmt = transMgr.dbCon.prepareStatement(VOL_SELECT_BY_RES);
        stmt.setString(1, resRef.getAssignedNode().getName().value);
        stmt.setString(2, resRef.getDefinition().getName().value);
        ResultSet resultSet = stmt.executeQuery();

        List<VolumeData> ret = load(accCtx, resRef, resultSet, serialGen, transMgr);
        resultSet.close();
        stmt.close();

        return ret;
    }


    private static List<VolumeData> load(
        AccessContext accCtx,
        Resource resRef,
        ResultSet resultSet,
        SerialGenerator serialGen,
        TransactionMgr transMgr
    )
        throws SQLException
    {
        List<VolumeData> volList = new ArrayList<>();
        while (resultSet.next())
        {
            VolumeDefinition volDfn = null;
            VolumeNumber volNr;
            try
            {
                volNr = new VolumeNumber(resultSet.getInt(VOL_ID));
            }
            catch (ValueOutOfRangeException valueOutOfRangeExc)
            {
                throw new DrbdSqlRuntimeException(
                    String.format("Invalid volumeNumber in %s: %d", TBL_VOL, resultSet.getInt(VOL_ID)),
                    valueOutOfRangeExc
                );
            }
            VolumeDefinitionDataDatabaseDriver volDfnDriver = DrbdManage.getVolumeDefinitionDataDatabaseDriver();
            volDfn = volDfnDriver.load(
                resRef.getDefinition(),
                volNr,
                serialGen,
                transMgr
            );

            VolumeData volData = cacheGet(resRef.getAssignedNode(), resRef.getDefinition(), volNr);
            if (volData == null)
            {
                try
                {
                    volData = new VolumeData(
                        UuidUtils.asUuid(resultSet.getBytes(VOL_UUID)),
                        resRef,
                        volDfn,
                        resultSet.getString(VOL_BLOCK_DEVICE),
                        resultSet.getString(VOL_META_DISK),
                        resultSet.getLong(VOL_FLAGS),
                        accCtx,
                        serialGen,
                        transMgr
                    );
                }
                catch (AccessDeniedException accessDeniedExc)
                {
                    handleAccessDeniedException(accessDeniedExc);
                }
                if (!cache(volData, accCtx))
                {
                    volData = cacheGet(resRef.getAssignedNode(), resRef.getDefinition(), volNr);
                }
                else
                {
                    // restore flags
                    StateFlags<VlmFlags> flags = volData.getFlags();
                    long lFlags = resultSet.getLong(VOL_FLAGS);
                    for (VlmFlags flag : VlmFlags.values())
                    {
                        if ((lFlags & flag.flagValue) == flag.flagValue)
                        {
                            try
                            {
                                flags.enableFlags(accCtx, flag);
                            }
                            catch (AccessDeniedException accessDeniedExc)
                            {
                                throw new ImplementationError(
                                    "Database's access context has no permission to set VolumeDefinition's flags",
                                    accessDeniedExc
                                );
                            }
                        }
                    }
                }
            }
            volList.add(volData);
        }
        // resultSet should be closed by caller of this method
        return volList;
    }

    @Override
    public void create(VolumeData vol, TransactionMgr transMgr) throws SQLException
    {
        try(PreparedStatement stmt = transMgr.dbCon.prepareStatement(VOL_INSERT))
        {
            stmt.setBytes(1, UuidUtils.asByteArray(vol.getUuid()));
            stmt.setString(2, vol.getResource().getAssignedNode().getName().value);
            stmt.setString(3, vol.getResourceDfn().getName().value);
            stmt.setInt(4, vol.getVolumeDefinition().getVolumeNumber(dbCtx).value);
            stmt.setString(5, vol.getBlockDevicePath(dbCtx));
            stmt.setString(6, vol.getMetaDiskPath(dbCtx));
            stmt.setLong(7, vol.getFlags().getFlagsBits(dbCtx));
            stmt.executeUpdate();
            cache(vol, dbCtx);
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            throw new ImplementationError(
                "Database's access context has no permission to access Volume",
                accessDeniedExc
            );
        }
    }

    @Override
    public void delete(VolumeData volume, TransactionMgr transMgr) throws SQLException
    {
        try(PreparedStatement stmt = transMgr.dbCon.prepareStatement(VOL_DELETE))
        {
            stmt.setString(1, volume.getResource().getAssignedNode().getName().value);
            stmt.setString(2, volume.getResource().getDefinition().getName().value);
            stmt.setInt(3, volume.getVolumeDefinition().getVolumeNumber(dbCtx).value);
            stmt.executeUpdate();

            cacheRemove(volume.getResource(), volume.getVolumeDefinition().getVolumeNumber(dbCtx));
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            throw new ImplementationError(
                "Database's access context has no permission to access Volume",
                accessDeniedExc
            );
        }
    }

    @Override
    public StateFlagsPersistence<VolumeData> getStateFlagsPersistence()
    {
        return flagPersistenceDriver;
    }

    private synchronized static boolean cache(VolumeData vol, AccessContext accCtx)
    {
        boolean contains = false;
        try
        {
            PrimaryKey pk = new PrimaryKey(
                vol.getResource().getAssignedNode().getName().value,
                vol.getResourceDfn().getName().value,
                vol.getVolumeDefinition().getVolumeNumber(accCtx).value);
            contains = volCache.containsKey(pk);
            if (!contains)
            {
                volCache.put(pk, vol);
            }
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            handleAccessDeniedException(accessDeniedExc);
        }
        return !contains;
    }

    private static VolumeData cacheGet(Node node, ResourceDefinition resDfn, VolumeNumber volNr)
    {
        return volCache.get(new PrimaryKey(node.getName().value, resDfn.getName().value, volNr.value));
    }

    private synchronized static void cacheRemove(Resource res, VolumeNumber volumeNumber)
    {
        volCache.remove(getPk(res, volumeNumber));
    }

    private static PrimaryKey getPk(Resource res, VolumeNumber volumeNumber)
    {
        return new PrimaryKey(
            res.getAssignedNode().getName().value,
            res.getDefinition().getName().value,
            volumeNumber.value
        );
    }

    /**
     * this method should only be called by tests or if you want a full-reload from the database
     */
    static synchronized void clearCache()
    {
        volCache.clear();
    }

    private static void handleAccessDeniedException(AccessDeniedException accessDeniedExc) throws ImplementationError
    {
        throw new ImplementationError(
            "Database's access context has no permission to acces VolumeData's volNumber",
            accessDeniedExc
        );
    }

    private class VolFlagsPersistence implements StateFlagsPersistence<VolumeData>
    {
        @Override
        public void persist(VolumeData volume, long flags, TransactionMgr transMgr)
            throws SQLException
        {
            try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(VOL_UPDATE_FLAGS))
            {
                stmt.setLong(1, flags);
                stmt.setString(2, volume.getResource().getAssignedNode().getName().value);
                stmt.setString(3, volume.getResource().getDefinition().getName().value);
                stmt.setInt(4, volume.getVolumeDefinition().getVolumeNumber(dbCtx).value);
                stmt.executeUpdate();
            }
            catch (AccessDeniedException accessDeniedExc)
            {
                handleAccessDeniedException(accessDeniedExc);
            }
        }
    }
}
