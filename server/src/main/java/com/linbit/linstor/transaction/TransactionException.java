package com.linbit.linstor.transaction;

import com.linbit.linstor.LinStorRuntimeException;
import com.linbit.linstor.annotation.Nullable;

public class TransactionException extends LinStorRuntimeException
{
    public TransactionException(String message, @Nullable Throwable cause)
    {
        super(message, cause);
    }
}
