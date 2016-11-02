package com.linbit.drbd.md;

/**
 * Standard interface for implementations of DRBD data / meta data size calculations
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface MetaDataApi
{
    public enum SizeSpec
    {
        netSize,
        grossSize
    };

    public long getNetSize(long grossSize, short peers, int alStripes, long alStripeSize)
        throws IllegalArgumentException, MinSizeException, MaxSizeException,
               MinAlSizeException, MaxAlSizeException, AlStripesException, PeerCountException;

    public long getGrossSize(long netSize, short peers, int alStripes, long alStripeSize)
        throws IllegalArgumentException, MinSizeException, MaxSizeException,
               MinAlSizeException, MaxAlSizeException, AlStripesException, PeerCountException;

    public long getInternalMdSize(
        SizeSpec mode,
        long     size,
        short    peers,
        int      alStripes,
        long     alStripeSize
    )
        throws IllegalArgumentException, MinSizeException, MaxSizeException,
               MinAlSizeException, MaxAlSizeException, AlStripesException, PeerCountException;

    public long getExternalMdSize(long size, short peers, int alStripes, long alStripeSize)
        throws IllegalArgumentException, MinSizeException, MaxSizeException,
               MinAlSizeException, MaxAlSizeException, AlStripesException, PeerCountException;
}
