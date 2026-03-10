package com.linbit.drbd.md;

public class BitmapBlockSizeException extends MdException
{
    public BitmapBlockSizeException(String message)
    {
        super(message);
    }

    public BitmapBlockSizeException()
    {
        super("Invalid DRBD bitmap block size value");
    }
}
