package com.linbit.drbdmanage.dbdrivers.interfaces;

import java.sql.Connection;
import java.sql.SQLException;
import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.StorPoolData;
import com.linbit.drbdmanage.StorPoolName;
import com.linbit.drbdmanage.propscon.SerialGenerator;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;

public interface StorPoolDataDatabaseDriver
{
    public StorPoolData load(Connection con, AccessContext accCtx, TransactionMgr transMgr, SerialGenerator serGen)
        throws SQLException, AccessDeniedException;

    public void create(Connection dbCon, StorPoolData storPoolData)
        throws SQLException;

    public void delete(Connection con, StorPoolName name)
        throws SQLException;

    public void ensureEntryExists(Connection con, StorPoolData storPoolData)
        throws SQLException;

}
