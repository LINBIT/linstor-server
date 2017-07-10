package com.linbit.drbdmanage.dbdrivers.interfaces;

import java.sql.Connection;
import java.sql.SQLException;

import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.VolumeData;
import com.linbit.drbdmanage.propscon.SerialGenerator;
import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;

public interface VolumeDataDatabaseDriver
{
    public StateFlagsPersistence getStateFlagsPersistence();

    public PropsConDatabaseDriver getPropsConDriver();

    public VolumeData load(Connection dbCon, TransactionMgr transMgr, SerialGenerator srlGen)
        throws SQLException;

    public void create(Connection dbCon, VolumeData vol)
        throws SQLException;

    public void delete(Connection con)
        throws SQLException;

}
