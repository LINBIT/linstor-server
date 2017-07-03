package com.linbit.drbdmanage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.linbit.ImplementationError;
import com.linbit.ObjectDatabaseDriver;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.drbdmanage.dbdrivers.derby.DerbyConstants;
import com.linbit.drbdmanage.dbdrivers.interfaces.VolumeDefinitionDataDatabaseDriver;
import com.linbit.drbdmanage.propscon.SerialGenerator;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;

public class VolumeDataDefinitionDerbyDriver implements VolumeDefinitionDataDatabaseDriver
{
    private static final String TBL_VOL_DFN = DerbyConstants.TBL_VOLUME_DEFINITIONS;

    private static final String VD_UUID = DerbyConstants.UUID;
    private static final String VD_RES_NAME = DerbyConstants.RESOURCE_NAME;
    private static final String VD_VOL_ID = DerbyConstants.VLM_ID;
    private static final String VD_VOL_SIZE = DerbyConstants.VLM_SIZE;
    private static final String VD_VOL_MINOR_NR = DerbyConstants.VLM_MINOR_NR;

    private static final String VD_SELECT =
        " SELECT " + VD_UUID + ", " + VD_VOL_SIZE + ", " + VD_VOL_MINOR_NR +
        " FROM " + TBL_VOL_DFN +
        " WHERE " + VD_RES_NAME + " = ? AND " +
                    VD_VOL_ID   + " = ?";

    private AccessContext dbCtx;
    private ResourceDefinition resDfn;
    private VolumeNumber volNr;

    public VolumeDataDefinitionDerbyDriver(AccessContext accCtx, ResourceDefinition resDfnRef, VolumeNumber volNrRef)
    {
        dbCtx = accCtx;
        resDfn = resDfnRef;
        volNr = volNrRef;
    }

    public VolumeDefinitionData load(Connection con, SerialGenerator serialGen) throws SQLException
    {
        PreparedStatement stmt = con.prepareStatement(VD_SELECT);
        stmt.setString(1, resDfn.getName().value);
        stmt.setInt(2, volNr.value);
        ResultSet resultSet = stmt.executeQuery();

        VolumeDefinitionData ret = null;

        if (resultSet.next())
        {
            try
            {
                ret = new VolumeDefinitionData(
                    dbCtx, // volumeDefinition does not have objProt, but require access to their resource's objProt
                    resDfn,
                    volNr,
                    new MinorNumber(resultSet.getInt(VD_VOL_MINOR_NR)),
                    resultSet.getLong(VD_VOL_SIZE),
                    serialGen
                );
            }
            catch (AccessDeniedException accessDeniedExc)
            {
                throw new ImplementationError(
                    "Database's access context has no permission to create VolumeDefinition",
                    accessDeniedExc
                );
            }
            catch (MdException mdExc)
            {
                throw new DrbdSqlRuntimeException(
                    "Invalid volume size: " + resultSet.getLong(VD_VOL_SIZE),
                    mdExc
                );
            }
            catch (ValueOutOfRangeException valueOutOfRangeExc)
            {
                throw new DrbdSqlRuntimeException(
                    "Invalid minor number: " + resultSet.getInt(VD_VOL_MINOR_NR),
                    valueOutOfRangeExc
                );
            }
        }


        return ret;
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

}
