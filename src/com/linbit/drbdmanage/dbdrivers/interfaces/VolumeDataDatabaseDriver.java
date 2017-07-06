package com.linbit.drbdmanage.dbdrivers.interfaces;

import java.sql.Connection;
import java.sql.SQLException;

import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.Resource;
import com.linbit.drbdmanage.VolumeData;
import com.linbit.drbdmanage.VolumeDefinition;
import com.linbit.drbdmanage.propscon.SerialGenerator;
import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;

public interface VolumeDataDatabaseDriver
{
    public StateFlagsPersistence getStateFlagsPersistence();

    public PropsConDatabaseDriver getPropsConDriver(Resource resRef, VolumeDefinition volDfnRef);

    public VolumeData load(
        Connection dbCon,
        Resource resRef,
        VolumeDefinition volDfn,
        TransactionMgr transMgr,
        SerialGenerator srlGen
    )
        throws SQLException;

    public void create(Connection dbCon, VolumeData vol)
        throws SQLException;

    public void delete(Connection con, VolumeData vol)
        throws SQLException;
}
