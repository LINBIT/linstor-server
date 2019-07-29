package com.linbit.linstor.core.apicallhandler.response;

import com.linbit.linstor.transaction.TransactionException;

public class ApiTransactionException extends ApiException
{
    public ApiTransactionException(TransactionException sqlExceptionRef)
    {
        super(sqlExceptionRef);
    }
}
