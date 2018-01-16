package com.linbit.linstor;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.TransactionMgr;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.VolumeDefinition.VlmDfnFlags;
import com.linbit.linstor.dbdrivers.DerbyDriver;
import com.linbit.linstor.dbdrivers.derby.DerbyConstants;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDefinitionDataDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.utils.StringUtils;
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
        " (" +
            VD_UUID + ", " + VD_RES_NAME + ", " + VD_ID + ", " +
            VD_SIZE + ", " + VD_MINOR_NR  + ", " +  VD_FLAGS +
        ") VALUES (?, ?, ?, ?, ?, ?)";
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
        try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(VD_INSERT))
        {
            errorReporter.logTrace("Creating VolumeDefinition %s", getId(volumeDefinition));

            stmt.setBytes(1, UuidUtils.asByteArray(volumeDefinition.getUuid()));
            stmt.setString(2, volumeDefinition.getResourceDefinition().getName().value);
            stmt.setInt(3, volumeDefinition.getVolumeNumber().value);
            stmt.setLong(4, volumeDefinition.getVolumeSize(dbCtx));
            stmt.setInt(5, volumeDefinition.getMinorNr(dbCtx).value);
            stmt.setLong(6, volumeDefinition.getFlags().getFlagsBits(dbCtx));

            stmt.executeUpdate();

            errorReporter.logTrace("VolumeDefinition created %s", getId(volumeDefinition));
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
        boolean logWarnIfNotExists,
        TransactionMgr transMgr
    )
        throws SQLException
    {
        errorReporter.logTrace("Loading VolumeDefinition %s", getId(resourceDefinition, volumeNumber));
        VolumeDefinitionData ret = null;
        try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(VD_SELECT))
        {
            stmt.setString(1, resourceDefinition.getName().value);
            stmt.setInt(2, volumeNumber.value);
            try (ResultSet resultSet = stmt.executeQuery())
            {
                ret = cacheGet(resourceDefinition, volumeNumber);
                if (ret == null)
                {
                    if (resultSet.next())
                    {
                        ret = restoreVolumeDefinition(
                            resultSet,
                            resourceDefinition,
                            volumeNumber,
                            transMgr,
                            dbCtx
                        );
                        errorReporter.logTrace("VolumeDefinition loaded %s", getId(resourceDefinition, volumeNumber));
                    }
                    else
                    if (logWarnIfNotExists)
                    {
                        errorReporter.logWarning(
                            "Requested VolumeDefinition %s could not be found in the Database",
                            getId(resourceDefinition, volumeNumber)
                        );
                    }
                }
            }
        }
        return ret;
    }

    private VolumeDefinitionData restoreVolumeDefinition(
        ResultSet resultSet,
        ResourceDefinition resDfn,
        VolumeNumber volNr,
        TransactionMgr transMgr,
        AccessContext accCtx
    )
        throws SQLException
    {
        errorReporter.logTrace("Restoring VolumeDefinition %s", getId(resDfn, volNr));
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
                    transMgr
                );
                errorReporter.logTrace("VolumeDefinition %s created during restore", getId(ret));
                // restore references
            }
            else
            {
                errorReporter.logTrace("VolumeDefinition %s restored from cache", getId(ret));
            }
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            DerbyDriver.handleAccessDeniedException(accessDeniedExc);
        }
        catch (MdException mdExc)
        {
            throw new LinStorSqlRuntimeException(
                String.format(
                    "A VolumeSize of a stored VolumeDefinition in table %s could not be restored. " +
                        "(ResourceName=%s, VolumeNumber=%d, invalid VolumeSize=%d)",
                    TBL_VOL_DFN,
                    resDfn.getName().value,
                    volNr.value,
                    resultSet.getLong(VD_SIZE)
                ),
                mdExc
            );
        }
        catch (ValueOutOfRangeException valueOutOfRangeExc)
        {
            throw new LinStorSqlRuntimeException(
                String.format(
                    "A MinorNumber of a stored VolumeDefinition in table %s could not be restored. " +
                        "(ResourceName=%s, VolumeNumber=%d, invalid MinorNumber=%d)",
                    TBL_VOL_DFN,
                    resDfn.getName().value,
                    volNr.value,
                    resultSet.getLong(VD_MINOR_NR)
                ),
                valueOutOfRangeExc
            );
        }
        return ret;
    }


    public List<VolumeDefinition> loadAllVolumeDefinitionsByResourceDefinition(
            ResourceDefinition resDfn,
            TransactionMgr transMgr,
            AccessContext accCtx
    )
        throws SQLException
    {
        errorReporter.logTrace(
            "Loading list of VolumeDefinitions by ResourceDefinition (ResName=%s)",
            resDfn.getName().value
        );
        List<VolumeDefinition> ret = new ArrayList<>();
        try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(VD_SELECT_BY_RES_DFN))
        {
            stmt.setString(1, resDfn.getName().value);
            try (ResultSet resultSet = stmt.executeQuery())
            {
                while (resultSet.next())
                {
                    VolumeNumber volNr;
                    try
                    {
                        volNr = new VolumeNumber(resultSet.getInt(VD_ID));
                    }
                    catch (ValueOutOfRangeException valueOutOfRangeExc)
                    {
                        throw new LinStorSqlRuntimeException(
                            String.format(
                                "A VolumeNumber of a stored VolumeDefinition in table %s could not be restored. " +
                                    "(ResourceName=%s, invalid VolumeNumber=%d)",
                                TBL_VOL_DFN,
                                resDfn.getName().value,
                                resultSet.getLong(VD_ID)
                            ),
                            valueOutOfRangeExc
                        );
                    }

                    VolumeDefinitionData volDfn = restoreVolumeDefinition(
                        resultSet,
                        resDfn,
                        volNr,
                        transMgr,
                        accCtx
                    );
                    errorReporter.logTrace("VolumeDefinition created %s", getId(volDfn));

                    ret.add(volDfn);
                }
            }
        }
        return ret;
    }

    @Override
    public void delete(VolumeDefinitionData volumeDefinition, TransactionMgr transMgr) throws SQLException
    {
        errorReporter.logTrace("Deleting VolumeDefinition %s", getId(volumeDefinition));
        try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(VD_DELETE))
        {
            stmt.setString(1, volumeDefinition.getResourceDefinition().getName().value);
            stmt.setInt(2, volumeDefinition.getVolumeNumber().value);
            stmt.executeUpdate();
            errorReporter.logTrace("VolumeDefinition deleted %s", getId(volumeDefinition));
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

    private String getId(VolumeDefinitionData volDfn)
    {
        return getId(
            volDfn.getResourceDefinition(),
            volDfn.getVolumeNumber()
        );
    }

    private String getId(ResourceDefinition resourceDefinition, VolumeNumber volumeNumber)
    {
        return getId(
            resourceDefinition.getName().displayValue,
            volumeNumber
        );
    }

    private String getId(String resName, VolumeNumber volNum)
    {
        return "(ResName=" + resName + " VolNum=" + volNum.value + ")";
    }

    private class FlagDriver implements StateFlagsPersistence<VolumeDefinitionData>
    {
        @Override
        public void persist(VolumeDefinitionData volumeDefinition, long flags, TransactionMgr transMgr) throws SQLException
        {
            try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(VD_UPDATE_FLAGS))
            {
                String fromFlags = StringUtils.join(
                    FlagsHelper.toStringList(
                        VlmDfnFlags.class,
                        volumeDefinition.getFlags().getFlagsBits(dbCtx)
                    ),
                    ", "
                );
                String toFlags = StringUtils.join(
                    FlagsHelper.toStringList(
                        VlmDfnFlags.class,
                        flags
                    ),
                    ", "
                );
                errorReporter.logTrace(
                    "Updating VolumeDefinition's flags from [%s] to [%s] %s",
                    fromFlags,
                    toFlags,
                    getId(volumeDefinition)
                );
                stmt.setLong(1, flags);
                stmt.setString(2, volumeDefinition.getResourceDefinition().getName().value);
                stmt.setInt(3, volumeDefinition.getVolumeNumber().value);
                stmt.executeUpdate();

                errorReporter.logTrace(
                    "VolumeDefinition's flags updated from [%s] to [%s] %s",
                    fromFlags,
                    toFlags,
                    getId(volumeDefinition)
                );
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
                errorReporter.logTrace(
                    "Updating VolumeDefinition's MinorNumber from [%d] to [%d] %s",
                    volumeDefinition.getMinorNr(dbCtx).value,
                    newNumber.value,
                    getId(volumeDefinition)
                );
                stmt.setInt(1, newNumber.value);
                stmt.setString(2, volumeDefinition.getResourceDefinition().getName().value);
                stmt.setInt(3, volumeDefinition.getVolumeNumber().value);
                stmt.executeUpdate();

                errorReporter.logTrace(
                    "VolumeDefinition's MinorNumber updated from [%d] to [%d] %s",
                    volumeDefinition.getMinorNr(dbCtx).value,
                    newNumber.value,
                    getId(volumeDefinition)
                );
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
                errorReporter.logTrace(
                    "Updating VolumeDefinition's Size from [%d] to [%d] %s",
                    volumeDefinition.getVolumeSize(dbCtx),
                    size,
                    getId(volumeDefinition)
                );

                stmt.setLong(1, size);
                stmt.setString(2, volumeDefinition.getResourceDefinition().getName().value);
                stmt.setInt(3, volumeDefinition.getVolumeNumber().value);
                stmt.executeUpdate();

                errorReporter.logTrace(
                    "VolumeDefinition's Size updated from [%d] to [%d] %s",
                    volumeDefinition.getVolumeSize(dbCtx),
                    size,
                    getId(volumeDefinition)
                );
            }
            catch (AccessDeniedException accessDeniedExc)
            {
                DerbyDriver.handleAccessDeniedException(accessDeniedExc);
            }
        }
    }
}
