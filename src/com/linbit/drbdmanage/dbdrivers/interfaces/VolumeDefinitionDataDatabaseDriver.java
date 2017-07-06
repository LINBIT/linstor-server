package com.linbit.drbdmanage.dbdrivers.interfaces;

import java.sql.Connection;
import java.sql.SQLException;

import com.linbit.ObjectDatabaseDriver;
import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.MinorNumber;
import com.linbit.drbdmanage.VolumeDefinitionData;
import com.linbit.drbdmanage.propscon.SerialGenerator;
import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;

public interface VolumeDefinitionDataDatabaseDriver
{
    public StateFlagsPersistence getStateFlagsPersistence();

    public ObjectDatabaseDriver<MinorNumber> getMinorNumberDriver();

    public ObjectDatabaseDriver<Long> getVolumeSizeDriver();

    public PropsConDatabaseDriver getPropsDriver();

    public void create(Connection con, VolumeDefinitionData volDfnData)
        throws SQLException;

    public VolumeDefinitionData load(Connection con, TransactionMgr transMgr, SerialGenerator serialGen)
        throws SQLException;

    public void delete(Connection con)
        throws SQLException;
}
