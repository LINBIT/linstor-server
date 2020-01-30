package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.core.apicallhandler.response.ApiTransactionException;
import com.linbit.linstor.transaction.TransactionException;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

@Singleton
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
        catch (TransactionException sqlExc)
        {
            throw new ApiTransactionException(sqlExc);
        }
    }

    public void rollback()
    {
        try
        {
            transMgrProvider.get().rollback();
        }
        catch (TransactionException sqlExc)
        {
            throw new ApiTransactionException(sqlExc);
        }
    }
}
