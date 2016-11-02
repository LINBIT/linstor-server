package com.linbit.drbd.md;

public class MinSizeException extends MdException
{
    public MinSizeException(String message)
    {
        super(message);
    }

    public MinSizeException()
    {
        super("DRBD device size is too small");
    }
}
