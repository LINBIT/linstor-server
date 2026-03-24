package com.linbit.linstor.layer.storage;

public class BlockSizeConsts
{
    /*
     * Minimum IO Size, aka physical_io_size constants
     */
    /** Minimum value for physical_io_size */
    public static final long MIN_PHY_IO_SIZE    = (1L << 9);
    /** Default value for physical_io_size, within [MIN_PHY_IO_SIZE, MAX_PHY_IO_SIZE]*/
    public static final long DFLT_PHY_IO_SIZE   = (1L << 9);
    /** Maximum value for physical_io_size*/
    public static final long MAX_PHY_IO_SIZE    = (1L << 12);
    /** Default value for the physical_io_size value of non-storage layers*/
    public static final long DFLT_SPECIAL_PHY_IO_SIZE = (1L << 12);

    /*
     * Discard Granularity, aka discard_granularity constants
     */
    /**
     * Minimum value for discard_granularity.
     * NOTE: 0 means "no discard support"
     */
    public static final long MIN_DISC_GRAN = 0;
    /**
     * Default value for discard_granularity, within [MIN_DISC_GRAN, MAX_DISC_GRAN]
     * NOTE: 0 means "no discard support"
     */
    public static final long DFLT_DISC_GRAN = 0;
    /** Maximum value for discard_granularity (currently unbounded / bounded by long) */
    public static final long MAX_DISC_GRAN = Long.MAX_VALUE;

    /*
     * Optimal IO Size, aka optimal_io_size constants
     */
    /**
     * Minimum value for optimal_io_size.
     * NOTE: 0 means "no recommendation"
     */
    public static final long MIN_OPT_IO_SIZE = 0;
    /**
     * Default value for optimal_io_size, within [MIN_OPT_IO_SIZE, MAX_OPT_IO_SIZE]
     * NOTE: 0 means "no recommendation"
     */
    public static final long DFLT_OPT_IO_SIZE = 0;
    /** Maximum value for optimal_io_size (currently unbounded / bounded by long) */
    public static final long MAX_OPT_IO_SIZE = Long.MAX_VALUE;

}
