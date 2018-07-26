package com.linbit.drbd.md;

public class MaxSizeException extends MdException
{
    public MaxSizeException(String message)
    {
        super(message);
    }

    public MaxSizeException()
    {
        super("DRBD device size is too big");
    }
}
