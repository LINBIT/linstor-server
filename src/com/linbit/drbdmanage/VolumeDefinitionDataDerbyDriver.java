package com.linbit.drbdmanage;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.TransactionMgr;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.drbdmanage.dbdrivers.DerbyDriver;
import com.linbit.drbdmanage.dbdrivers.derby.DerbyConstants;
import com.linbit.drbdmanage.dbdrivers.interfaces.VolumeDefinitionDataDatabaseDriver;
import com.linbit.drbdmanage.logging.ErrorReporter;
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
    private static final String VD_UPDATE_FLAGS =
        " UPDATE " + TBL_VOL_DFN +
        " SET " + VD_FLAGS + " = ? " +
        " WHERE " + VD_RES_NAME + " = ? AND " +
                    VD_ID       + " = ?";
    private static final String VD_UPDATE_MINOR_NR =
        " UPDATE " + TBL_VOL_DFN +
        " SET " + VD_MINOR_NR + " = ? " +
        " WHERE " + VD_RES_NAME + " = ? AND " +
                    VD_ID       + " = ?";
    private static final String VD_UPDATE_SIZE =
        " UPDATE " + TBL_VOL_DFN +
        " SET " + VD_SIZE + " = ? " +
        " WHERE " + VD_RES_NAME + " = ? AND " +
                    VD_ID       + " = ?";
    private static final String VD_DELETE =
        " DELETE FROM " + TBL_VOL_DFN +
        " WHERE " + VD_RES_NAME + " = ? AND " +
                    VD_ID       + " = ?";

    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;

    private final FlagDriver flagsDriver;
    private final MinorNumberDriver minorNumberDriver;
    private final SizeDriver sizeDriver;

    public VolumeDefinitionDataDerbyDriver(AccessContext accCtx, ErrorReporter errorReporterRef)
    {
        dbCtx = accCtx;
        errorReporter = errorReporterRef;
        flagsDriver = new FlagDriver();
        minorNumberDriver = new MinorNumberDriver();
        sizeDriver = new SizeDriver();
    }

    @Override
    public void create(VolumeDefinitionData volumeDefinition, TransactionMgr transMgr) throws SQLException
    {
        try(PreparedStatement stmt = transMgr.dbCon.prepareStatement(VD_INSERT))
        {
            stmt.setBytes(1, UuidUtils.asByteArray(volumeDefinition.getUuid()));
            stmt.setString(2, volumeDefinition.getResourceDefinition().getName().value);
            stmt.setInt(3, volumeDefinition.getVolumeNumber(dbCtx).value);
            stmt.setLong(4, volumeDefinition.getVolumeSize(dbCtx));
            stmt.setInt(5, volumeDefinition.getMinorNr(dbCtx).value);
            stmt.setLong(6, volumeDefinition.getFlags().getFlagsBits(dbCtx));

            stmt.executeUpdate();
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            DerbyDriver.handleAccessDeniedException(accessDeniedExc);
        }
    }

    @Override
    public VolumeDefinitionData load(
        ResourceDefinition resourceDefinition,
        VolumeNumber volumeNumber,
        SerialGenerator serialGen,
        TransactionMgr transMgr
    )
        throws SQLException
    {
        PreparedStatement stmt = transMgr.dbCon.prepareStatement(VD_SELECT);
        stmt.setString(1, resourceDefinition.getName().value);
        stmt.setInt(2, volumeNumber.value);
        ResultSet resultSet = stmt.executeQuery();

        VolumeDefinitionData ret = cacheGet(resourceDefinition, volumeNumber);

        if (ret == null)
        {
            if (resultSet.next())
            {
                ret = restoreVolumeDefinition(
                    resultSet,
                    resourceDefinition,
                    volumeNumber,
                    serialGen,
                    transMgr,
                    dbCtx
                );
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

        return ret;
    }

    private VolumeDefinitionData restoreVolumeDefinition(
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
            ret = cacheGet(resDfn, volNr);

            if (ret == null)
            {
                ret = new VolumeDefinitionData(
                    UuidUtils.asUuid(resultSet.getBytes(VD_UUID)),
                    accCtx, // volumeDefinition does not have objProt, but require access to their resource's objProt
                    resDfn,
                    volNr,
                    new MinorNumber(resultSet.getInt(VD_MINOR_NR)),
                    resultSet.getLong(VD_SIZE),
                    resultSet.getLong(VD_FLAGS),
                    serialGen,
                    transMgr
                );
                // restore references
            }
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            DerbyDriver.handleAccessDeniedException(accessDeniedExc);
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


    public List<VolumeDefinition> loadAllVolumeDefinitionsByResourceDefinition(
            ResourceDefinition resDfn,
            SerialGenerator serialGen,
            TransactionMgr transMgr,
            AccessContext accCtx
    )
        throws SQLException
    {
        PreparedStatement stmt = transMgr.dbCon.prepareStatement(VD_SELECT_BY_RES_DFN);
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
    public void delete(VolumeDefinitionData volumeDefinition, TransactionMgr transMgr) throws SQLException
    {
        try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(VD_DELETE))
        {
            stmt.setString(1, volumeDefinition.getResourceDefinition().getName().value);
            stmt.setInt(2, volumeDefinition.getVolumeNumber(dbCtx).value);
            stmt.executeUpdate();
        }
        catch (AccessDeniedException accDeniedExc)
        {
            DerbyDriver.handleAccessDeniedException(accDeniedExc);
        }
    }

    @Override
    public StateFlagsPersistence<VolumeDefinitionData> getStateFlagsPersistence()
    {
        return flagsDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<VolumeDefinitionData, MinorNumber> getMinorNumberDriver()
    {
        return minorNumberDriver;
    }

    @Override
    public SingleColumnDatabaseDriver<VolumeDefinitionData, Long> getVolumeSizeDriver()
    {
        return sizeDriver;
    }

    private VolumeDefinitionData cacheGet(ResourceDefinition resDfn, VolumeNumber volNr)
    {
        VolumeDefinitionData ret = null;

        try
        {
            ret = (VolumeDefinitionData) resDfn.getVolumeDfn(dbCtx, volNr);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            DerbyDriver.handleAccessDeniedException(accDeniedExc);
        }

        return ret;
    }

    private class FlagDriver implements StateFlagsPersistence<VolumeDefinitionData>
    {
        @Override
        public void persist(VolumeDefinitionData volumeDefinition, long flags, TransactionMgr transMgr) throws SQLException
        {
            try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(VD_UPDATE_FLAGS))
            {
                stmt.setLong(1, flags);
                stmt.setString(2, volumeDefinition.getResourceDefinition().getName().value);
                stmt.setInt(3, volumeDefinition.getVolumeNumber(dbCtx).value);
                stmt.executeUpdate();
            }
            catch (AccessDeniedException accessDeniedExc)
            {
                DerbyDriver.handleAccessDeniedException(accessDeniedExc);
            }
        }
    }

    private class MinorNumberDriver implements SingleColumnDatabaseDriver<VolumeDefinitionData, MinorNumber>
    {
        @Override
        public void update(VolumeDefinitionData volumeDefinition, MinorNumber newNumber, TransactionMgr transMgr)
            throws SQLException
        {
            try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(VD_UPDATE_MINOR_NR))
            {
                stmt.setInt(1, newNumber.value);
                stmt.setString(2, volumeDefinition.getResourceDefinition().getName().value);
                stmt.setInt(3, volumeDefinition.getVolumeNumber(dbCtx).value);
                stmt.executeUpdate();
            }
            catch (AccessDeniedException accessDeniedExc)
            {
                DerbyDriver.handleAccessDeniedException(accessDeniedExc);
            }
        }
    }

    private class SizeDriver implements SingleColumnDatabaseDriver<VolumeDefinitionData, Long>
    {
        @Override
        public void update(VolumeDefinitionData volumeDefinition, Long size, TransactionMgr transMgr) throws SQLException
        {
            try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(VD_UPDATE_SIZE))
            {
                stmt.setLong(1, size);
                stmt.setString(2, volumeDefinition.getResourceDefinition().getName().value);
                stmt.setInt(3, volumeDefinition.getVolumeNumber(dbCtx).value);
                stmt.executeUpdate();
            }
            catch (AccessDeniedException accessDeniedExc)
            {
                DerbyDriver.handleAccessDeniedException(accessDeniedExc);
            }
        }
    }
}
