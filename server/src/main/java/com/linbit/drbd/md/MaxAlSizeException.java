package com.linbit.drbd.md;

public class MaxAlSizeException extends MdException
{
    public MaxAlSizeException(String message)
    {
        super(message);
    }

    public MaxAlSizeException()
    {
        super("Activity log size is too big");
    }
}
