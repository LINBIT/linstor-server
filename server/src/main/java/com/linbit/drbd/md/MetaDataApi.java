package com.linbit.drbd.md;

/**
 * Standard interface for implementations of DRBD data / meta data size calculations
 *
 * @author raltnoeder
 */
public interface MetaDataApi
{
    enum SizeSpec
    {
        NET_SIZE,
        GROSS_SIZE
    }

    long getNetSize(long grossSize, short peers, int alStripes, long alStripeSize, int bitmapBlockSize)
        throws MinSizeException, MaxSizeException,
               MinAlSizeException, MaxAlSizeException, AlStripesException,
               PeerCountException, BitmapBlockSizeException;

    long getGrossSize(long netSize, short peers, int alStripes, long alStripeSize, int bitmapBlockSize)
        throws MinSizeException, MaxSizeException,
               MinAlSizeException, MaxAlSizeException, AlStripesException,
               PeerCountException, BitmapBlockSizeException;

    long getInternalMdSize(
        SizeSpec mode,
        long     size,
        short    peers,
        int      alStripes,
        long     alStripeSize,
        int      bitmapBlockSize
    )
        throws IllegalArgumentException, MinSizeException, MaxSizeException,
               MinAlSizeException, MaxAlSizeException, AlStripesException,
               PeerCountException, BitmapBlockSizeException;

    long getExternalMdSize(long size, short peers, int alStripes, long alStripeSize, int bitmapBlockSize)
        throws MinSizeException, MaxSizeException,
               MinAlSizeException, MaxAlSizeException, AlStripesException,
               PeerCountException, BitmapBlockSizeException;
}
