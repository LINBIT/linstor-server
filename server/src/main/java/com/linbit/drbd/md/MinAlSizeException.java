package com.linbit.drbd.md;

public class MinAlSizeException extends MdException
{
    public MinAlSizeException(String message)
    {
        super(message);
    }

    public MinAlSizeException()
    {
        super("Activity log size is too small");
    }
}
