package com.linbit.drbdmanage;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.linbit.ImplementationError;
import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.TransactionMgr;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.drbdmanage.dbdrivers.DerbyDriver;
import com.linbit.drbdmanage.dbdrivers.derby.DerbyConstants;
import com.linbit.drbdmanage.dbdrivers.interfaces.VolumeDefinitionDataDatabaseDriver;
import com.linbit.drbdmanage.logging.ErrorReporter;
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
            errorReporter.logTrace("Creating VolumeDefinition %s", getTraceId(volumeDefinition));

            stmt.setBytes(1, UuidUtils.asByteArray(volumeDefinition.getUuid()));
            stmt.setString(2, volumeDefinition.getResourceDefinition().getName().value);
            stmt.setInt(3, volumeDefinition.getVolumeNumber(dbCtx).value);
            stmt.setLong(4, volumeDefinition.getVolumeSize(dbCtx));
            stmt.setInt(5, volumeDefinition.getMinorNr(dbCtx).value);
            stmt.setLong(6, volumeDefinition.getFlags().getFlagsBits(dbCtx));

            stmt.executeUpdate();

            errorReporter.logTrace("VolumeDefinition created %s", getDebugId(volumeDefinition));
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
        errorReporter.logTrace("Loading VolumeDefinition %s", getTraceId(resourceDefinition, volumeNumber));
        VolumeDefinitionData ret = null;
        try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(VD_SELECT))
        {
            stmt.setString(1, resourceDefinition.getName().value);
            stmt.setInt(2, volumeNumber.value);
            try (ResultSet resultSet = stmt.executeQuery())
            {
                ret = cacheGet(resourceDefinition, volumeNumber);
                if (resultSet.next())
                {
                    ret = restoreVolumeDefinition(
                        resultSet,
                        resourceDefinition,
                        volumeNumber,
                        transMgr,
                        dbCtx
                    );
                    errorReporter.logTrace("VolumeDefinition loaded %s", getDebugId(resourceDefinition, volumeNumber));
                }
                else
                if (logWarnIfNotExists)
                {
                    errorReporter.logWarning(
                        "Requested VolumeDefinition %s could not be found in the Database",
                        getDebugId(resourceDefinition, volumeNumber)
                    );
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
        errorReporter.logTrace("Restoring VolumeDefinition %s", getTraceId(resDfn, volNr));
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
                errorReporter.logTrace("VolumeDefinition %s created during restore", getTraceId(ret));
                // restore references
            }
            else
            {
                errorReporter.logTrace("VolumeDefinition %s restored from cache", getTraceId(ret));
            }
        }
        catch (AccessDeniedException accessDeniedExc)
        {
            DerbyDriver.handleAccessDeniedException(accessDeniedExc);
        }
        catch (MdException mdExc)
        {
            throw new DrbdSqlRuntimeException(
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
            throw new DrbdSqlRuntimeException(
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
                        throw new DrbdSqlRuntimeException(
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
                    errorReporter.logTrace("VolumeDefinition created %s", getTraceId(volDfn));

                    ret.add(volDfn);
                }
            }
        }
        return ret;
    }

    @Override
    public void delete(VolumeDefinitionData volumeDefinition, TransactionMgr transMgr) throws SQLException
    {
        errorReporter.logTrace("Deleting VolumeDefinition %s", getTraceId(volumeDefinition));
        try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(VD_DELETE))
        {
            stmt.setString(1, volumeDefinition.getResourceDefinition().getName().value);
            stmt.setInt(2, volumeDefinition.getVolumeNumber(dbCtx).value);
            stmt.executeUpdate();
            errorReporter.logTrace("VolumeDefinition deleted %s", getDebugId(volumeDefinition));
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

    private String getDebugId(VolumeDefinitionData volDfn)
    {
        return getDebugId(
            volDfn.getResourceDefinition(),
            getVolNr(volDfn)
        );
    }

    private String getDebugId(ResourceDefinition resourceDefinition, VolumeNumber volumeNumber)
    {
        return getId(
            resourceDefinition.getName().displayValue,
            volumeNumber
        );
    }

    private String getTraceId(VolumeDefinitionData volDfn)
    {
        return getTraceId(
            volDfn.getResourceDefinition(),
            getVolNr(volDfn)
        );
    }

    private String getTraceId(ResourceDefinition resourceDefinition, VolumeNumber volumeNumber)
    {
        return getId(
            resourceDefinition.getName().value,
            volumeNumber
        );
    }

    private VolumeNumber getVolNr(VolumeDefinitionData volDfn) throws ImplementationError
    {
        VolumeNumber volumeNumber = null;
        try
        {
            volumeNumber = volDfn.getVolumeNumber(dbCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            DerbyDriver.handleAccessDeniedException(accDeniedExc);
        }
        return volumeNumber;
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
                errorReporter.logTrace(
                    "Updating VolumeDefinition's flags from [%s] to [%s] %s",
                    Long.toBinaryString(volumeDefinition.getFlags().getFlagsBits(dbCtx)),
                    Long.toBinaryString(flags),
                    getTraceId(volumeDefinition)
                );
                stmt.setLong(1, flags);
                stmt.setString(2, volumeDefinition.getResourceDefinition().getName().value);
                stmt.setInt(3, volumeDefinition.getVolumeNumber(dbCtx).value);
                stmt.executeUpdate();

                errorReporter.logTrace(
                    "VolumeDefinition's flags updated from [%s] to [%s] %s",
                    Long.toBinaryString(volumeDefinition.getFlags().getFlagsBits(dbCtx)),
                    Long.toBinaryString(flags),
                    getDebugId(volumeDefinition)
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
                    getTraceId(volumeDefinition)
                );
                stmt.setInt(1, newNumber.value);
                stmt.setString(2, volumeDefinition.getResourceDefinition().getName().value);
                stmt.setInt(3, volumeDefinition.getVolumeNumber(dbCtx).value);
                stmt.executeUpdate();

                errorReporter.logTrace(
                    "VolumeDefinition's MinorNumber updated from [%d] to [%d] %s",
                    volumeDefinition.getMinorNr(dbCtx).value,
                    newNumber.value,
                    getDebugId(volumeDefinition)
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
                    getTraceId(volumeDefinition)
                );

                stmt.setLong(1, size);
                stmt.setString(2, volumeDefinition.getResourceDefinition().getName().value);
                stmt.setInt(3, volumeDefinition.getVolumeNumber(dbCtx).value);
                stmt.executeUpdate();

                errorReporter.logTrace(
                    "VolumeDefinition's Size updated from [%d] to [%d] %s",
                    volumeDefinition.getVolumeSize(dbCtx),
                    size,
                    getDebugId(volumeDefinition)
                );
            }
            catch (AccessDeniedException accessDeniedExc)
            {
                DerbyDriver.handleAccessDeniedException(accessDeniedExc);
            }
        }
    }
}
