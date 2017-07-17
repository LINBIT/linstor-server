package com.linbit.drbdmanage.dbdrivers.interfaces;

import java.sql.Connection;
import java.sql.SQLException;

import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.ConnectionDefinitionData;
import com.linbit.drbdmanage.propscon.SerialGenerator;

public interface ConnectionDefinitionDataDatabaseDriver
{
    public void create(Connection con, ConnectionDefinitionData conDfnData)
        throws SQLException;

    public ConnectionDefinitionData load(
        Connection con,
        SerialGenerator serialGen,
        TransactionMgr transMgr
    )
        throws SQLException;


    // update makes no sense. currently ConDfnData contains uuid and 3 primary keys.
    // nothing that should be updatable
    //    public void update(Connection con, ConnectionDefinitionData conDfnData)
    //        throws SQLException;

    public void delete(Connection con)
        throws SQLException;


}
