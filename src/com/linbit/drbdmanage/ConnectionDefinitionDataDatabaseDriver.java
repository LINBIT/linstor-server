package com.linbit.drbdmanage;

import java.sql.Connection;
import java.sql.SQLException;

import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.propscon.SerialGenerator;

public interface ConnectionDefinitionDataDatabaseDriver
{
    public ConnectionDefinitionData load(
        Connection con,
        ResourceName resName,
        NodeName srcNodeName,
        NodeName dstNodeName,
        TransactionMgr transMgr,
        SerialGenerator serialGen
    )
        throws SQLException;

    public void create(Connection con, ConnectionDefinitionData conDfnData)
        throws SQLException;

    // update makes no sense. currently ConDfnData contains uuid and 3 primary keys.
    // nothing that should be updatable
    //    public void update(Connection con, ConnectionDefinitionData conDfnData)
    //        throws SQLException;

    public void delete(Connection con, ConnectionDefinitionData conDfnData)
        throws SQLException;


}
