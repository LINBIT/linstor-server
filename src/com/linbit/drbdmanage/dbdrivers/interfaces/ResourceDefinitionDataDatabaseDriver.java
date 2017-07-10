package com.linbit.drbdmanage.dbdrivers.interfaces;

import java.sql.Connection;
import java.sql.SQLException;

import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.ResourceDefinitionData;
import com.linbit.drbdmanage.propscon.SerialGenerator;
import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;

public interface ResourceDefinitionDataDatabaseDriver
{
    public StateFlagsPersistence getStateFlagsPersistence();

    public PropsConDatabaseDriver getPropsConDriver();

    public void create(Connection dbCon, ResourceDefinitionData resDfn)
        throws SQLException;

    public boolean exists(Connection dbCon)
        throws SQLException;

    public ResourceDefinitionData load(Connection dbCon, SerialGenerator serialGen, TransactionMgr transMgr)
        throws SQLException;

    void delete(Connection con)
        throws SQLException;

}
