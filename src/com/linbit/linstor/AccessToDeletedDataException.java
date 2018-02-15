package com.linbit.linstor;

public class AccessToDeletedDataException extends LinStorRuntimeException
{
    private static final long serialVersionUID = -4216737156323935538L;

    public AccessToDeletedDataException(String message)
    {
        super(message, null);
    }
}
