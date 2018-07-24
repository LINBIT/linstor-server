package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.core.apicallhandler.response.ApiSQLException;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;
import java.sql.SQLException;

public class CtrlTransactionHelper
{
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public CtrlTransactionHelper(Provider<TransactionMgr> transMgrProviderRef)
    {
        transMgrProvider = transMgrProviderRef;
    }

    public void commit()
    {
        try
        {
            transMgrProvider.get().commit();
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
    }
}
