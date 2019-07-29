package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.LinStorException;

public class DatabaseException extends LinStorException
{
    public DatabaseException(Throwable cause)
    {
        super("DatabaseException", cause);
    }
}
