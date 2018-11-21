package com.linbit.linstor.transaction;

public class TransactionException extends RuntimeException
{
    public TransactionException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
