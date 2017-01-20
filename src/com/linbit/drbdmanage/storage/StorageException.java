package com.linbit.drbdmanage.storage;

public class StorageException extends Exception
{
    public StorageException()
    {
    }

    public StorageException(String message)
    {
        super(message);
    }

    public StorageException(String message, Exception nestedException)
    {
        super(message, nestedException);
    }
}
