package com.linbit.drbdmanage;

import java.sql.Connection;
import java.sql.SQLException;

import com.linbit.drbdmanage.dbdrivers.derby.DerbyConstants;
import com.linbit.drbdmanage.dbdrivers.interfaces.VolumeDataDatabaseDriver;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;

public class VolumeDataDerbyDriver implements VolumeDataDatabaseDriver
{
    private static final String TBL_VOL_DFN = DerbyConstants.TBL_VOLUME_DEFINITIONS;

    private AccessContext privCtx;

    public VolumeDataDerbyDriver(AccessContext privCtx)
    {
        this.privCtx = privCtx;
    }

    @Override
    public StateFlagsPersistence getStateFlagsPersistence()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public VolumeData load(Connection dbCon, Resource resRef, VolumeDefinition volDfn) throws SQLException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void create(Connection dbCon, VolumeData vol) throws SQLException
    {
        // TODO Auto-generated method stub

    }

}
