package com.linbit.drbdmanage.dbdrivers.interfaces;

import java.sql.Connection;
import java.sql.SQLException;

import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.StorPoolData;
import com.linbit.drbdmanage.propscon.SerialGenerator;

public interface StorPoolDataDatabaseDriver
{
    public StorPoolData load(Connection con, TransactionMgr transMgr, SerialGenerator serGen)
        throws SQLException;

    public void create(Connection dbCon, StorPoolData storPoolData)
        throws SQLException;

    public void delete(Connection con)
        throws SQLException;

    public void ensureEntryExists(Connection con, StorPoolData storPoolData)
        throws SQLException;

}
