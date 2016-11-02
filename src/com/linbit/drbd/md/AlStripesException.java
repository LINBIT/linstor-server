package com.linbit.drbd.md;

public class AlStripesException extends MdException
{
    public AlStripesException(String message)
    {
        super(message);
    }

    public AlStripesException()
    {
        super("Activity log stripes count is out of range");
    }
}
