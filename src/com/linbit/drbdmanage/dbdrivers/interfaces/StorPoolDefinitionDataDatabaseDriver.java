package com.linbit.drbdmanage.dbdrivers.interfaces;

import java.sql.Connection;
import java.sql.SQLException;

import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.StorPoolDefinitionData;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;

public interface StorPoolDefinitionDataDatabaseDriver
{
    public void create(Connection con, StorPoolDefinitionData storPoolDefinitionData)
        throws SQLException;

    public StorPoolDefinitionData load(Connection con, AccessContext accCtx, TransactionMgr transMgr)
        throws SQLException, AccessDeniedException;

    public void delete(Connection con)
        throws SQLException;
}
