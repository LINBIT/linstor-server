package com.linbit.linstor.transaction;

import com.linbit.linstor.LinStorRuntimeException;

public class TransactionException extends LinStorRuntimeException
{
    public TransactionException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
