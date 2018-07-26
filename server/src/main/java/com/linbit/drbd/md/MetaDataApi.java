package com.linbit.drbd.md;

/**
 * Standard interface for implementations of DRBD data / meta data size calculations
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface MetaDataApi
{
    enum SizeSpec
    {
        NET_SIZE,
        GROSS_SIZE
    }

    long getNetSize(long grossSize, short peers, int alStripes, long alStripeSize)
        throws IllegalArgumentException, MinSizeException, MaxSizeException,
               MinAlSizeException, MaxAlSizeException, AlStripesException, PeerCountException;

    long getGrossSize(long netSize, short peers, int alStripes, long alStripeSize)
        throws IllegalArgumentException, MinSizeException, MaxSizeException,
               MinAlSizeException, MaxAlSizeException, AlStripesException, PeerCountException;

    long getInternalMdSize(
        SizeSpec mode,
        long     size,
        short    peers,
        int      alStripes,
        long     alStripeSize
    )
        throws IllegalArgumentException, MinSizeException, MaxSizeException,
               MinAlSizeException, MaxAlSizeException, AlStripesException, PeerCountException;

    long getExternalMdSize(long size, short peers, int alStripes, long alStripeSize)
        throws IllegalArgumentException, MinSizeException, MaxSizeException,
               MinAlSizeException, MaxAlSizeException, AlStripesException, PeerCountException;
}
