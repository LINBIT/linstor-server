package com.linbit.linstor.layer.storage;

public class BlockSizeConsts
{
    // Minimum value for minimum_io_size
    public static final long MIN_IO_SIZE    = (1L << 9);

    // Default value for minimum_io_size, within [MIN_IO_SIZE, MAX_IO_SIZE]
    public static final long DFLT_IO_SIZE   = (1L << 9);

    // Maximum value for minimum_io_size
    public static final long MAX_IO_SIZE    = (1L << 12);

    // Default value for the minimum_io_size value of non-storage layers
    public static final long DFLT_SPECIAL_IO_SIZE   = (1L << 12);
}
